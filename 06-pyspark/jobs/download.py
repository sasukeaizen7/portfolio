"""Download NYC yellow-taxi trips (January-June 2024, ~20M trips, ~300 MB of Parquet) and the zone table."""

from __future__ import annotations

import requests

from .common import RAW

BASE = "https://d37ci6vzurychx.cloudfront.net"
MONTHS = [f"2024-{m:02d}" for m in range(1, 7)]


def fetch(url: str, dest) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    tmp.rename(dest)
    print(f"  downloaded {dest.name} ({dest.stat().st_size / 1e6:.0f} MB)")


def main() -> None:
    for month in MONTHS:
        fetch(f"{BASE}/trip-data/yellow_tripdata_{month}.parquet", RAW / "yellow" / f"yellow_tripdata_{month}.parquet")
    fetch(f"{BASE}/misc/taxi_zone_lookup.csv", RAW / "taxi_zone_lookup.csv")


if __name__ == "__main__":
    main()
