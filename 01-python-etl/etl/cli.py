"""Command line entry point: python -m etl.cli --start 2024-01-01 --end 2024-12-31 [--postgres]"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .extract import CITIES, fetch_daily_weather
from .load import upsert_postgres, write_parquet
from .transform import to_frame

log = logging.getLogger("etl")


def run(cities: list[str], start: date, end: date, out: Path, dsn: str | None) -> pd.DataFrame:
    frames = []
    for key in cities:
        city = CITIES[key]
        payload = fetch_daily_weather(city, start, end)
        frames.append(to_frame(payload, city))
        log.info("extracted %s: %d days", city.name, len(frames[-1]))
    df = pd.concat(frames, ignore_index=True)
    files = write_parquet(df, out)
    log.info("wrote %d rows to %d Parquet partitions under %s", len(df), len(files), out)
    if dsn:
        n = upsert_postgres(df, dsn)
        log.info("upserted %d rows into Postgres", n)
    return df


def main(argv: list[str] | None = None) -> int:
    yesterday = date.today() - timedelta(days=1)
    p = argparse.ArgumentParser(description="Daily weather ETL: Open-Meteo -> Parquet -> Postgres")
    p.add_argument("--cities", default=",".join(CITIES), help=f"comma-separated, from: {', '.join(CITIES)}")
    p.add_argument("--start", type=date.fromisoformat, default=yesterday - timedelta(days=6))
    p.add_argument("--end", type=date.fromisoformat, default=yesterday)
    p.add_argument("--out", type=Path, default=Path("data"))
    p.add_argument("--postgres", action="store_true", help="also upsert into Postgres (DSN from $PG_DSN)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cities = [c.strip().lower() for c in args.cities.split(",") if c.strip()]
    unknown = [c for c in cities if c not in CITIES]
    if unknown:
        p.error(f"unknown cities: {unknown}")
    dsn = None
    if args.postgres:
        dsn = os.environ.get("PG_DSN")
        if not dsn:
            p.error("--postgres needs the PG_DSN environment variable")
    try:
        run(cities, args.start, args.end, args.out, dsn)
    except Exception:
        log.exception("ETL failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
