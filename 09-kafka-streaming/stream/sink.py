"""The consumer's write path, independent of Kafka (so it is unit-tested without a broker).

Delivery semantics: Kafka gives AT-LEAST-ONCE delivery here (offsets are committed only after the
database transaction commits, so a crash replays messages). The sink makes the RESULT exactly-once:
  * events are inserted with ON CONFLICT (event_id) DO NOTHING -> a replayed or duplicated event is ignored;
  * the per-minute aggregates are incremented ONLY from the rows that were actually inserted
    (INSERT ... RETURNING), in the same transaction -> counts never double either.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime

from .events import ClickEvent, InvalidEvent, parse

DDL = """
CREATE SCHEMA IF NOT EXISTS clicks;
CREATE TABLE IF NOT EXISTS clicks.events (
    event_id    text PRIMARY KEY,
    event_time  timestamptz NOT NULL,
    user_id     text NOT NULL,
    session_id  text NOT NULL,
    event_type  text NOT NULL,
    page        text NOT NULL,
    product_id  text,
    price       numeric(8, 2),
    kafka_partition int,
    kafka_offset    bigint,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_time ON clicks.events (event_time);
CREATE TABLE IF NOT EXISTS clicks.minute_counts (
    minute     timestamptz NOT NULL,
    event_type text        NOT NULL,
    events     bigint      NOT NULL,
    revenue    numeric(12, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (minute, event_type)
);
CREATE TABLE IF NOT EXISTS clicks.dead_letters (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw         text NOT NULL,
    error       text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
)
"""


def ensure_schema(conn) -> None:
    """Several consumers start at once: an advisory lock makes them create the schema one at a time
    (concurrent CREATE ... IF NOT EXISTS can still collide in Postgres)."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(90909)")
        for statement in DDL.split(";"):
            if statement.strip():
                cur.execute(statement)
    conn.commit()


def write_batch(conn, messages: list[tuple[bytes, int, int]]) -> dict:
    """messages: (value, partition, offset). One transaction for the whole batch.
    Returns counts plus the invalid payloads, which the caller also publishes to the DLQ topic."""
    valid: list[tuple[ClickEvent, int, int]] = []
    invalid: list[tuple[bytes, str]] = []
    for value, partition, offset in messages:
        try:
            valid.append((parse(value), partition, offset))
        except InvalidEvent as e:
            invalid.append((value, str(e)))

    inserted: list[tuple[datetime, str, float | None]] = []
    with conn.cursor() as cur:
        for e, partition, offset in valid:
            cur.execute(
                "INSERT INTO clicks.events (event_id, event_time, user_id, session_id, event_type, page, product_id, "
                "price, kafka_partition, kafka_offset) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (event_id) DO NOTHING RETURNING date_trunc('minute', event_time), event_type, price",
                (e.event_id, e.event_time, e.user_id, e.session_id, e.event_type, e.page, e.product_id, e.price,
                 partition, offset))
            row = cur.fetchone()
            if row:
                inserted.append(row)
        counts = Counter((minute, kind) for minute, kind, _ in inserted)
        revenue = Counter()
        for minute, kind, price in inserted:
            if kind == "purchase" and price is not None:
                revenue[(minute, kind)] += float(price)
        for (minute, kind), n in counts.items():
            cur.execute(
                "INSERT INTO clicks.minute_counts (minute, event_type, events, revenue) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (minute, event_type) DO UPDATE SET events = clicks.minute_counts.events + EXCLUDED.events, "
                "revenue = clicks.minute_counts.revenue + EXCLUDED.revenue",
                (minute, kind, n, round(revenue[(minute, kind)], 2)))
        for raw, error in invalid:
            cur.execute("INSERT INTO clicks.dead_letters (raw, error) VALUES (%s, %s)",
                        (raw.decode("utf-8", "replace"), error))
    conn.commit()
    return {"received": len(messages), "inserted": len(inserted), "duplicates": len(valid) - len(inserted),
            "invalid": invalid}
