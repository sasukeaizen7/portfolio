"""The two pipeline steps the DAGs call (kept here so they run and are tested without Airflow)."""

from __future__ import annotations

from . import lake, warehouse
from .gbfs import take_snapshot


def capture(s3) -> str:
    """API -> S3 raw zone. Returns the object key."""
    lake.ensure_bucket(s3)
    return lake.put_snapshot(s3, take_snapshot())


def load_new(s3, conn) -> dict:
    """S3 raw zone -> warehouse: every snapshot in the bucket that isn't loaded yet (so a missed run
    catches up automatically, and re-running loads nothing twice)."""
    warehouse.ensure_schema(conn)
    done = warehouse.loaded_ids(conn)
    pending = [k for k in lake.list_snapshot_keys(s3) if k.rsplit("snapshot_", 1)[1][:-8] not in done]
    rows = sum(warehouse.load_snapshot(conn, lake.get_snapshot(s3, k), k) for k in pending)
    return {"snapshots_loaded": len(pending), "station_rows": rows}
