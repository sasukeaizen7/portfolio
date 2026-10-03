"""Monitoring and alerting: pipeline health checks, and an alert sink.

Checks run every 30 minutes (monitoring DAG); a failed check raises an alert. Alerts always go to the
monitoring.alerts table (visible on the dashboard) and, if ALERT_WEBHOOK_URL is set, are POSTed as
JSON to that URL (Slack, Teams, Discord and most chat tools accept incoming webhooks).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import requests

DDL = """
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raised_at  timestamptz NOT NULL DEFAULT now(),
    severity   text NOT NULL CHECK (severity IN ('warning', 'critical')),
    source     text NOT NULL,
    message    text NOT NULL
)
"""


@dataclass
class CheckResult:
    name: str
    ok: bool
    severity: str
    detail: str


CHECKS = [
    # (name, severity, SQL returning (ok boolean, detail text))
    ("fresh data: a snapshot in the last 30 minutes", "critical",
     "SELECT coalesce(max(taken_at) > now() - interval '30 minutes', false), "
     "'latest snapshot: ' || coalesce(max(taken_at)::text, 'none') FROM raw.velib_snapshots"),
    ("complete snapshots: >= 1,000 stations each (last 24 h)", "warning",
     "SELECT coalesce(min(stations) >= 1000, true), 'smallest snapshot: ' || coalesce(min(stations)::text, '-') || ' stations' "
     "FROM raw.velib_snapshots WHERE taken_at > now() - interval '24 hours'"),
    ("network not frozen: counts changed in the last hour", "warning",
     "SELECT count(DISTINCT mechanical + 1000 * ebike) > 1, count(DISTINCT snapshot_id) || ' snapshots checked' "
     "FROM raw.velib_station_status WHERE taken_at > now() - interval '1 hour'"),
    ("no station over capacity (bikes + docks <= capacity + 2)", "warning",
     "SELECT count(*) = 0, count(*) || ' station-snapshots over capacity' FROM raw.velib_station_status s "
     "JOIN raw.velib_station_information i USING (station_id) "
     "WHERE s.taken_at > now() - interval '24 hours' AND s.mechanical + s.ebike + s.docks > i.capacity + 2"),
]


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for statement in DDL.split(";"):
            if statement.strip():
                cur.execute(statement)
    conn.commit()


def run_checks(conn) -> list[CheckResult]:
    results = []
    with conn.cursor() as cur:
        for name, severity, sql in CHECKS:
            cur.execute(sql)
            ok, detail = cur.fetchone()
            results.append(CheckResult(name, bool(ok), severity, detail))
    return results


def raise_alert(conn, severity: str, source: str, message: str, webhook: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO monitoring.alerts (severity, source, message) VALUES (%s, %s, %s)",
                    (severity, source, message))
    conn.commit()
    url = webhook if webhook is not None else os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        try:
            requests.post(url, data=json.dumps({"text": f"[{severity.upper()}] {source}: {message}"}),
                          headers={"Content-Type": "application/json"}, timeout=10)
        except requests.RequestException:
            pass          # an alerting failure must never hide the original problem: it's in the table
