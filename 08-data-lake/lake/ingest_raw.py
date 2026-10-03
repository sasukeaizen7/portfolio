"""Raw zone ingestion (the role an AWS Lambda or a scheduled job plays): copy source files into
s3://lake-raw unchanged, under an ingestion-date partition. The raw zone is immutable: a file is never
modified, a re-delivery is a new object, so any later step can be replayed from it.

    python -m lake.ingest_raw --months 2024-01 2024-02 ...
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import date

import requests

from .s3 import RAW_BUCKET, client

SOURCE = "https://s3.amazonaws.com/tripdata/JC-{yyyymm}-citibike-tripdata.csv.zip"


def raw_key(month: str, ingest_date: date) -> str:
    yyyymm = month.replace("-", "")
    return f"citibike/jersey_city/ingest_date={ingest_date.isoformat()}/JC-{yyyymm}-citibike-tripdata.csv.zip"


def ingest(month: str, s3=None, today: date | None = None, content: bytes | None = None) -> str:
    s3 = s3 or client()
    if content is None:
        r = requests.get(SOURCE.format(yyyymm=month.replace("-", "")), timeout=120)
        r.raise_for_status()
        content = r.content
    key = raw_key(month, today or date.today())
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=content,
                  Metadata={"source-month": month, "sha256": hashlib.sha256(content).hexdigest()})
    print(f"  s3://{RAW_BUCKET}/{key} ({len(content) / 1e6:.1f} MB)")
    return key


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--months", nargs="+", default=[f"2024-{m:02d}" for m in range(1, 13)])
    for month in p.parse_args().months:
        ingest(month)


if __name__ == "__main__":
    main()
