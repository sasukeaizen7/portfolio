"""Collect real snapshots on a laptop before the full stack is running (GBFS keeps no history, so the
earlier you start, the more history the dashboard has). The files go to data/landing/ and are
uploaded to the S3 raw zone by `python -m velib.lake upload-landing`.

    python scripts/collect_snapshots.py --every 300
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))
from velib.gbfs import FeedError, take_snapshot  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--every", type=int, default=300, help="seconds between snapshots")
    p.add_argument("--count", type=int, default=0, help="0 = forever")
    args = p.parse_args()
    out = ROOT / "data" / "landing"
    taken = 0
    while not args.count or taken < args.count:
        started = time.monotonic()
        try:
            snap = take_snapshot()
            folder = out / f"date={snap.taken_at:%Y-%m-%d}"
            folder.mkdir(parents=True, exist_ok=True)
            (folder / f"snapshot_{snap.snapshot_id}.json").write_bytes(snap.to_json())
            taken += 1
            print(f"{snap.snapshot_id}: {len(snap.status['data']['stations'])} stations", flush=True)
        except FeedError as e:
            print(f"skipped: {e}", flush=True)
        if args.count and taken >= args.count:
            break
        time.sleep(max(0, args.every - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
