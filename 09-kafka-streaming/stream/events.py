"""Click events: a realistic fake generator, and validation shared by producer and consumer."""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

EVENT_TYPES = ("page_view", "product_view", "add_to_cart", "purchase")
PRODUCTS = {f"P{i:03d}": round(random.Random(i).uniform(5, 120), 2) for i in range(1, 51)}


@dataclass
class ClickEvent:
    event_id: str
    event_time: str          # ISO 8601, UTC
    user_id: str
    session_id: str
    event_type: str
    page: str
    product_id: str | None
    price: float | None

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()


class InvalidEvent(ValueError):
    pass


def parse(raw: bytes) -> ClickEvent:
    """bytes from Kafka -> a valid ClickEvent, or InvalidEvent (the consumer routes those to the DLQ)."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise InvalidEvent(f"not JSON: {e}") from None
    if not isinstance(data, dict):
        raise InvalidEvent("not a JSON object")
    missing = [f for f in ("event_id", "event_time", "user_id", "session_id", "event_type", "page") if not data.get(f)]
    if missing:
        raise InvalidEvent(f"missing fields {missing}")
    if data["event_type"] not in EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type {data['event_type']!r}")
    try:
        datetime.fromisoformat(data["event_time"])
    except (TypeError, ValueError):
        raise InvalidEvent(f"bad event_time {data['event_time']!r}") from None
    if data["event_type"] in ("product_view", "add_to_cart", "purchase") and data.get("product_id") not in PRODUCTS:
        raise InvalidEvent(f"{data['event_type']} without a known product_id")
    return ClickEvent(**{k: data.get(k) for k in ClickEvent.__dataclass_fields__})


class ClickGenerator:
    """Users browsing a shop in sessions: views -> some product views -> some carts -> few purchases.

    Real streams are messy, so the generator also emits (at small, configurable rates):
      * DUPLICATES (the same event sent twice, as a producer retry would);
      * LATE events (event_time minutes in the past, as from a phone that was offline);
      * MALFORMED payloads (which must end up in the dead-letter topic, not crash the consumer).
    """

    def __init__(self, seed: int = 7, users: int = 500, duplicate_rate=0.01, late_rate=0.02, malformed_rate=0.002):
        self.rng = random.Random(seed)
        self.users = [f"U{i:04d}" for i in range(users)]
        self.duplicate_rate, self.late_rate, self.malformed_rate = duplicate_rate, late_rate, malformed_rate
        self.sessions: dict[str, tuple[str, int]] = {}      # user -> (session id, events left)

    def _event(self, now: datetime) -> ClickEvent:
        user = self.rng.choice(self.users)
        session, left = self.sessions.get(user, (None, 0))
        if left <= 0:
            session, left = uuid.UUID(int=self.rng.getrandbits(128)).hex[:12], self.rng.randint(3, 15)
        self.sessions[user] = (session, left - 1)
        roll = self.rng.random()
        kind = "purchase" if roll < 0.03 else "add_to_cart" if roll < 0.12 else "product_view" if roll < 0.55 else "page_view"
        product = self.rng.choice(list(PRODUCTS)) if kind != "page_view" else None
        when = now - timedelta(minutes=self.rng.randint(2, 30)) if self.rng.random() < self.late_rate else now
        return ClickEvent(
            event_id=uuid.UUID(int=self.rng.getrandbits(128)).hex,
            event_time=when.isoformat(timespec="milliseconds"),
            user_id=user, session_id=session, event_type=kind,
            page=f"/product/{product}" if product else self.rng.choice(["/", "/search", "/category/gifts", "/cart"]),
            product_id=product, price=PRODUCTS[product] if product else None,
        )

    def batch(self, n: int, now: datetime | None = None) -> list[tuple[str, bytes]]:
        """n messages as (key, value). The key is the user id: all of a user's events land in the same
        partition, so they stay in order for that user."""
        now = now or datetime.now(UTC)
        out: list[tuple[str, bytes]] = []
        for _ in range(n):
            if self.rng.random() < self.malformed_rate:
                out.append(("bad", b'{"event_type": "purchase", "oops'))
                continue
            e = self._event(now)
            out.append((e.user_id, e.to_json()))
            if self.rng.random() < self.duplicate_rate:
                out.append((e.user_id, e.to_json()))
        return out
