"""Load snapshots from the S3 raw zone into the warehouse's raw schema (the "Snowflake" stand-in: Postgres).

Idempotent: raw.velib_snapshots records every loaded snapshot; a snapshot already there is skipped,
and the per-station rows are keyed on (snapshot_id, station_id).
"""

from __future__ import annotations

import io

from .gbfs import Snapshot, information_rows, status_rows

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.velib_snapshots (
    snapshot_id text PRIMARY KEY,
    taken_at    timestamptz NOT NULL,
    stations    int NOT NULL,
    s3_key      text NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS raw.velib_station_status (
    snapshot_id   text        NOT NULL REFERENCES raw.velib_snapshots,
    taken_at      timestamptz NOT NULL,
    station_id    bigint      NOT NULL,
    station_code  text,
    mechanical    int         NOT NULL CHECK (mechanical >= 0),
    ebike         int         NOT NULL CHECK (ebike >= 0),
    docks         int         NOT NULL CHECK (docks >= 0),
    is_installed  boolean     NOT NULL,
    is_renting    boolean     NOT NULL,
    is_returning  boolean     NOT NULL,
    last_reported timestamptz,
    PRIMARY KEY (snapshot_id, station_id)
);
CREATE INDEX IF NOT EXISTS velib_status_taken_at ON raw.velib_station_status (taken_at);
CREATE TABLE IF NOT EXISTS raw.velib_station_information (
    station_id   bigint PRIMARY KEY,
    station_code text,
    name         text,
    lat          double precision NOT NULL,
    lon          double precision NOT NULL,
    capacity     int NOT NULL,
    seen_at      timestamptz NOT NULL          -- last snapshot that listed the station
)
"""

STATUS_COLS = ["snapshot_id", "taken_at", "station_id", "station_code", "mechanical", "ebike", "docks",
               "is_installed", "is_renting", "is_returning", "last_reported"]


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(31337)")
        for statement in DDL.split(";"):
            if statement.strip():
                cur.execute(statement)
    conn.commit()


def loaded_ids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT snapshot_id FROM raw.velib_snapshots")
        return {r[0] for r in cur.fetchall()}


def load_snapshot(conn, snap: Snapshot, s3_key: str) -> int:
    """One transaction per snapshot. Returns the number of station rows loaded (0 if already loaded)."""
    rows = status_rows(snap)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO raw.velib_snapshots (snapshot_id, taken_at, stations, s3_key) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id",
                    (snap.snapshot_id, snap.taken_at, len(rows), s3_key))
        if cur.fetchone() is None:
            conn.rollback()
            return 0
        buf = io.StringIO()
        for r in rows:
            buf.write("\t".join("\\N" if v is None else str(v) for v in r) + "\n")
        _copy(cur, f"COPY raw.velib_station_status ({', '.join(STATUS_COLS)}) FROM STDIN", buf.getvalue())
        cur.executemany(
            "INSERT INTO raw.velib_station_information VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (station_id) DO UPDATE SET station_code = EXCLUDED.station_code, name = EXCLUDED.name, "
            "lat = EXCLUDED.lat, lon = EXCLUDED.lon, capacity = EXCLUDED.capacity, seen_at = EXCLUDED.seen_at "
            "WHERE raw.velib_station_information.seen_at <= EXCLUDED.seen_at",
            information_rows(snap))
    conn.commit()
    return len(rows)


def _copy(cur, sql: str, data: str) -> None:
    """COPY FROM STDIN on psycopg2 (Airflow's hook) or psycopg 3 (tests, scripts)."""
    if hasattr(cur, "copy_expert"):
        cur.copy_expert(sql, io.StringIO(data))
    else:
        with cur.copy(sql) as cp:
            cp.write(data)
