"""Extract: the Vélib' Métropole GBFS feeds (Paris bike share, open data, no API key).

  * station_information: static-ish (name, position, capacity), ~1,500 stations;
  * station_status: live counts (bikes by type, free docks, is_renting...), refreshed every minute.

A snapshot = both feeds fetched together, validated, and flattened to one row per station.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import requests

BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
FEEDS = ("station_information", "station_status")
MIN_STATIONS = 1000            # the network has ~1,500: far fewer means a broken or partial feed


class FeedError(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    taken_at: datetime                 # when we fetched it (UTC)
    information: dict
    status: dict

    @property
    def snapshot_id(self) -> str:
        return self.taken_at.strftime("%Y%m%dT%H%M%SZ")

    def to_json(self) -> bytes:
        return json.dumps({"taken_at": self.taken_at.isoformat(), "information": self.information,
                           "status": self.status}).encode()

    @staticmethod
    def from_json(raw: bytes) -> Snapshot:
        d = json.loads(raw)
        return Snapshot(datetime.fromisoformat(d["taken_at"]), d["information"], d["status"])


def fetch_feed(name: str, session=None, attempts: int = 3) -> dict:
    http = session or requests.Session()
    for attempt in range(1, attempts + 1):
        try:
            r = http.get(f"{BASE}/{name}.json", timeout=30)
            if r.status_code == 200:
                return r.json()
            error = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            error = str(e)
        if attempt < attempts:
            time.sleep(2 * attempt)
    raise FeedError(f"{name}: {error}")


def take_snapshot(session=None, now: datetime | None = None) -> Snapshot:
    snap = Snapshot(now or datetime.now(UTC), fetch_feed("station_information", session),
                    fetch_feed("station_status", session))
    validate(snap)
    return snap


def validate(snap: Snapshot) -> None:
    info = snap.information.get("data", {}).get("stations")
    status = snap.status.get("data", {}).get("stations")
    if not info or not status:
        raise FeedError("a feed has no data.stations")
    if len(status) < MIN_STATIONS:
        raise FeedError(f"only {len(status)} stations in station_status (expected >= {MIN_STATIONS})")
    known = {s["station_id"] for s in info}
    unknown = sum(1 for s in status if s["station_id"] not in known)
    if unknown > 0.05 * len(status):
        raise FeedError(f"{unknown} status rows reference stations missing from station_information")


def bikes_by_type(row: dict) -> tuple[int, int]:
    """'num_bikes_available_types': [{'mechanical': 2}, {'ebike': 1}] -> (2, 1)."""
    types: dict[str, int] = {}
    for item in row.get("num_bikes_available_types") or []:
        types.update(item)
    return int(types.get("mechanical", 0)), int(types.get("ebike", 0))


def status_rows(snap: Snapshot) -> list[tuple]:
    """One row per station: (snapshot_id, taken_at, station_id, station_code, mechanical, ebike, docks,
    is_installed, is_renting, is_returning, last_reported)."""
    rows = []
    for s in snap.status["data"]["stations"]:
        mech, ebike = bikes_by_type(s)
        rows.append((snap.snapshot_id, snap.taken_at, int(s["station_id"]), str(s.get("stationCode")), mech, ebike,
                     int(s.get("num_docks_available") or 0), bool(s.get("is_installed")), bool(s.get("is_renting")),
                     bool(s.get("is_returning")),
                     datetime.fromtimestamp(s["last_reported"], UTC) if s.get("last_reported") else None))
    return rows


def information_rows(snap: Snapshot) -> list[tuple]:
    """(station_id, station_code, name, lat, lon, capacity, seen_at)."""
    return [(int(s["station_id"]), str(s.get("stationCode")), s.get("name"), float(s["lat"]), float(s["lon"]),
             int(s.get("capacity") or 0), snap.taken_at) for s in snap.information["data"]["stations"]]
