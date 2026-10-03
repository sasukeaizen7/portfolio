import copy
import dataclasses
import os
from datetime import UTC, datetime

import pytest
from velib import gbfs, lake, monitoring, warehouse


def test_real_snapshot_validates_and_flattens(snapshot):
    gbfs.validate(snapshot)
    rows = gbfs.status_rows(snapshot)
    assert len(rows) == 1519 and snapshot.snapshot_id == "20261003T160041Z"
    first = rows[0]
    assert first[2] == 213688169 and (first[4], first[5], first[6]) == (2, 1, 31)   # mechanical, ebike, docks
    assert len(gbfs.information_rows(snapshot)) == 1519


def test_partial_feed_is_refused(snapshot):
    broken = copy.deepcopy(snapshot.status)
    broken["data"]["stations"] = broken["data"]["stations"][:200]
    with pytest.raises(gbfs.FeedError, match="only 200 stations"):
        gbfs.validate(gbfs.Snapshot(snapshot.taken_at, snapshot.information, broken))


def test_bikes_by_type_handles_missing_types():
    assert gbfs.bikes_by_type({"num_bikes_available_types": [{"mechanical": 4}, {"ebike": 2}]}) == (4, 2)
    assert gbfs.bikes_by_type({"num_bikes_available_types": None}) == (0, 0)


def test_snapshot_roundtrip_through_json(snapshot):
    assert gbfs.Snapshot.from_json(snapshot.to_json()) == snapshot


def test_raw_zone_keys_are_date_partitioned():
    assert lake.key_for("20261003T160041Z") == "snapshots/date=2026-10-03/snapshot_20261003T160041Z.json.gz"


@pytest.mark.skipif(not os.environ.get("PG_DSN"), reason="needs a Postgres in $PG_DSN")
def test_s3_to_warehouse_is_idempotent_and_monitored(snapshot, monkeypatch):
    import psycopg
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=5012)
    server.start()
    try:
        monkeypatch.setenv("S3_ENDPOINT", "http://127.0.0.1:5012")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        s3 = lake.client()
        lake.ensure_bucket(s3)
        snapshot = dataclasses.replace(snapshot, taken_at=datetime(2025, 1, 15, 8, 0, tzinfo=UTC))   # an old one
        key = lake.put_snapshot(s3, snapshot)
        assert lake.list_snapshot_keys(s3) == [key]
        restored = lake.get_snapshot(s3, key)

        with psycopg.connect(os.environ["PG_DSN"]) as conn:
            for schema in ("raw", "monitoring"):
                conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.commit()
            warehouse.ensure_schema(conn)
            monitoring.ensure_schema(conn)
            assert warehouse.load_snapshot(conn, restored, key) == 1519
            assert warehouse.load_snapshot(conn, restored, key) == 0              # already loaded: skipped
            assert warehouse.loaded_ids(conn) == {"20250115T080000Z"}
            assert conn.execute("SELECT count(*), sum(mechanical + ebike) > 0 FROM raw.velib_station_status").fetchone() == (1519, True)

            results = {r.name: r for r in monitoring.run_checks(conn)}
            # The only snapshot is from January 2025 -> the freshness check must fail.
            assert not results["fresh data: a snapshot in the last 30 minutes"].ok
            monitoring.raise_alert(conn, "critical", "test", "stale data", webhook="")
            assert conn.execute("SELECT count(*) FROM monitoring.alerts").fetchone()[0] == 1
    finally:
        server.stop()
