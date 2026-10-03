"""The "Glue job": raw zipped CSVs -> typed, validated Parquet in the clean zone, partitioned by
year/month (Hive style), so query engines can skip partitions. Re-running a month replaces exactly
that month's partition (idempotent).

    python -m lake.raw_to_clean           # every raw file not yet processed (latest delivery per month)
"""

from __future__ import annotations

import io
import re
import tempfile
import zipfile
from pathlib import Path

from .s3 import CLEAN_BUCKET, RAW_BUCKET, client, duckdb_connection

KEY_RE = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})/JC-(\d{4})(\d{2})-citibike-tripdata\.csv\.zip$")


def latest_raw_files(s3) -> dict[tuple[str, str], str]:
    """(year, month) -> key of the most recent delivery of that month."""
    latest: dict[tuple[str, str], tuple[str, str]] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=RAW_BUCKET, Prefix="citibike/jersey_city/"):
        for obj in page.get("Contents", []):
            m = KEY_RE.search(obj["Key"])
            if m:
                ingest_date, year, month = m.groups()
                if (year, month) not in latest or ingest_date > latest[(year, month)][0]:
                    latest[(year, month)] = (ingest_date, obj["Key"])
    return {ym: key for ym, (_, key) in latest.items()}


CLEAN_SQL = """
SELECT
    ride_id,
    rideable_type,
    CAST(started_at AS TIMESTAMP)                                  AS started_at,
    CAST(ended_at AS TIMESTAMP)                                    AS ended_at,
    round(date_diff('second', CAST(started_at AS TIMESTAMP), CAST(ended_at AS TIMESTAMP)) / 60.0, 2) AS duration_min,
    start_station_id, start_station_name, end_station_id, end_station_name,
    CAST(start_lat AS DOUBLE) AS start_lat, CAST(start_lng AS DOUBLE) AS start_lng,
    CAST(end_lat AS DOUBLE)   AS end_lat,   CAST(end_lng AS DOUBLE)   AS end_lng,
    member_casual,
    {year}  AS year,
    {month} AS month
FROM trips_csv
WHERE ride_id IS NOT NULL
  AND CAST(ended_at AS TIMESTAMP) > CAST(started_at AS TIMESTAMP)
  AND date_diff('minute', CAST(started_at AS TIMESTAMP), CAST(ended_at AS TIMESTAMP)) <= 24 * 60
  AND year(CAST(started_at AS TIMESTAMP)) = {year} AND month(CAST(started_at AS TIMESTAMP)) = {month}
QUALIFY row_number() OVER (PARTITION BY ride_id ORDER BY started_at) = 1     -- de-duplicate ride ids
"""


def process(year: str, month: str, key: str, s3=None, con=None) -> dict:
    s3, con = s3 or client(), con or duckdb_connection()
    body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".csv") and not n.startswith("__MACOSX"))
        csv_bytes = z.read(csv_name)
    tmp = Path(tempfile.gettempdir(), f"lake_{year}{month}.csv")
    tmp.write_bytes(csv_bytes)
    con.execute(f"CREATE OR REPLACE TEMP TABLE trips_csv AS "
                f"SELECT * FROM read_csv('{tmp.as_posix()}', header = true, all_varchar = true)")
    tmp.unlink()
    raw_rows = con.execute("SELECT count(*) FROM trips_csv").fetchone()[0]
    con.execute(f"CREATE OR REPLACE TEMP TABLE trips_clean AS {CLEAN_SQL.format(year=int(year), month=int(month))}")
    clean_rows = con.execute("SELECT count(*) FROM trips_clean").fetchone()[0]

    prefix = f"citibike/trips/year={int(year)}/month={int(month)}/"
    # Overwrite the partition: delete what's there, then write the new file.
    old = s3.list_objects_v2(Bucket=CLEAN_BUCKET, Prefix=prefix).get("Contents", [])
    if old:
        s3.delete_objects(Bucket=CLEAN_BUCKET, Delete={"Objects": [{"Key": o["Key"]} for o in old]})
    con.execute(f"COPY (SELECT * EXCLUDE (year, month) FROM trips_clean ORDER BY started_at) "
                f"TO 's3://{CLEAN_BUCKET}/{prefix}part-0.parquet' (FORMAT parquet, COMPRESSION zstd)")
    print(f"  {year}-{month}: {raw_rows:,} raw -> {clean_rows:,} clean rows -> s3://{CLEAN_BUCKET}/{prefix}")
    return {"year": year, "month": month, "raw_rows": raw_rows, "clean_rows": clean_rows}


def main() -> None:
    s3, con = client(), duckdb_connection()
    for (year, month), key in sorted(latest_raw_files(s3).items()):
        process(year, month, key, s3, con)


if __name__ == "__main__":
    main()
