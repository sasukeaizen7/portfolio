"""The S3 raw zone (the "S3" of API -> Airflow -> S3 -> warehouse): every snapshot is kept, gzipped,
untouched, under a date partition. The warehouse can always be rebuilt from here.

    s3://velib-raw/snapshots/date=2026-10-03/snapshot_20261003T160041Z.json.gz
"""

from __future__ import annotations

import gzip
import os
import sys
from pathlib import Path

import boto3

from .gbfs import Snapshot

BUCKET = os.environ.get("VELIB_RAW_BUCKET", "velib-raw")
PREFIX = "snapshots/"


def client():
    return boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT"),
                        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                        region_name=os.environ.get("AWS_REGION", "us-east-1"))


def ensure_bucket(s3) -> None:
    if BUCKET not in {b["Name"] for b in s3.list_buckets().get("Buckets", [])}:
        s3.create_bucket(Bucket=BUCKET)


def key_for(snapshot_id: str) -> str:
    return f"{PREFIX}date={snapshot_id[:4]}-{snapshot_id[4:6]}-{snapshot_id[6:8]}/snapshot_{snapshot_id}.json.gz"


def put_snapshot(s3, snap: Snapshot) -> str:
    key = key_for(snap.snapshot_id)
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(snap.to_json()), ContentEncoding="gzip",
                  ContentType="application/json")
    return key


def get_snapshot(s3, key: str) -> Snapshot:
    return Snapshot.from_json(gzip.decompress(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))


def list_snapshot_keys(s3) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json.gz")]
    return sorted(keys)


def upload_landing(s3, landing: Path) -> int:
    """Upload snapshots collected by scripts/collect_snapshots.py (before the stack was running)."""
    ensure_bucket(s3)
    existing = set(list_snapshot_keys(s3))
    n = 0
    for path in sorted(landing.glob("date=*/snapshot_*.json")):
        snap = Snapshot.from_json(path.read_bytes())
        if key_for(snap.snapshot_id) not in existing:
            put_snapshot(s3, snap)
            n += 1
    return n


if __name__ == "__main__" and sys.argv[1:2] == ["upload-landing"]:
    print(f"uploaded {upload_landing(client(), Path(sys.argv[2] if len(sys.argv) > 2 else 'data/landing'))} snapshots")
