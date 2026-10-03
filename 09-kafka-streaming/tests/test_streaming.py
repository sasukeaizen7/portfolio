import json
import os
from collections import Counter
from datetime import UTC, datetime

import pytest
from stream.events import ClickGenerator, InvalidEvent, parse

NOW = datetime(2026, 10, 3, 12, 0, 30, tzinfo=UTC)


def test_generator_is_deterministic_and_keyed_by_user():
    a, b = ClickGenerator(seed=1).batch(200, NOW), ClickGenerator(seed=1).batch(200, NOW)
    assert a == b
    for key, value in a:
        if key != "bad":
            assert json.loads(value)["user_id"] == key


def test_generator_produces_the_documented_mess():
    msgs = ClickGenerator(seed=3, duplicate_rate=0.05, late_rate=0.1, malformed_rate=0.01).batch(5000, NOW)
    values = [v for _, v in msgs]
    dupes = sum(n - 1 for n in Counter(values).values() if n > 1)
    malformed = sum(1 for k, _ in msgs if k == "bad")
    late = sum(1 for k, v in msgs if k != "bad" and datetime.fromisoformat(json.loads(v)["event_time"]) < NOW)
    assert 150 < dupes < 350 and 20 < malformed and late > 300


@pytest.mark.parametrize("raw, message", [
    (b"not json", "not JSON"),
    (b"[1, 2]", "not a JSON object"),
    (json.dumps({"event_id": "x"}).encode(), "missing fields"),
    (json.dumps({"event_id": "x", "event_time": "2026-10-03T12:00:00+00:00", "user_id": "U1", "session_id": "s",
                 "event_type": "teleport", "page": "/"}).encode(), "unknown event_type"),
    (json.dumps({"event_id": "x", "event_time": "2026-10-03T12:00:00+00:00", "user_id": "U1", "session_id": "s",
                 "event_type": "purchase", "page": "/", "product_id": "NOPE"}).encode(), "without a known product_id"),
])
def test_invalid_events_are_rejected(raw, message):
    with pytest.raises(InvalidEvent, match=message):
        parse(raw)


@pytest.mark.skipif(not os.environ.get("PG_DSN"), reason="needs a Postgres in $PG_DSN")
def test_sink_is_exactly_once_under_duplicates_and_replays():
    import psycopg
    from stream.sink import ensure_schema, write_batch

    with psycopg.connect(os.environ["PG_DSN"]) as conn:
        conn.execute("DROP SCHEMA IF EXISTS clicks CASCADE")
        conn.commit()
        ensure_schema(conn)
        msgs = ClickGenerator(seed=5, duplicate_rate=0.05, malformed_rate=0.01).batch(1000, NOW)
        batch = [(v, i % 3, i) for i, (_, v) in enumerate(msgs)]
        first = write_batch(conn, batch)
        unique_valid = len({v for v in (m[0] for m in batch) if not v.startswith(b'{"event_type": "purchase", "oops')})
        assert first["inserted"] == unique_valid and first["duplicates"] > 0 and len(first["invalid"]) > 0

        replay = write_batch(conn, batch)                # a crash before the offset commit -> redelivery
        assert replay["inserted"] == 0

        events, counted, revenue, purchases = conn.execute("""
            SELECT (SELECT count(*) FROM clicks.events), (SELECT sum(events) FROM clicks.minute_counts),
                   (SELECT sum(revenue) FROM clicks.minute_counts),
                   (SELECT sum(price) FROM clicks.events WHERE event_type = 'purchase')""").fetchone()
        assert events == counted == unique_valid          # aggregates match the events: nothing double-counted
        assert revenue == purchases
        assert conn.execute("SELECT count(*) FROM clicks.dead_letters").fetchone()[0] == 2 * len(first["invalid"])
