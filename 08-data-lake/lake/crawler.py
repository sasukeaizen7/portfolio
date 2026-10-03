"""The "Glue crawler": scan the clean zone, infer each table's schema and partitions, and write a
catalog (s3://lake-clean/_catalog/catalog.json). Query engines read the catalog instead of guessing:
that is all a data catalog is, a shared answer to "which tables exist, where, with what columns".
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .s3 import CLEAN_BUCKET, client, duckdb_connection

CATALOG_KEY = "_catalog/catalog.json"
TABLES = {"citibike_trips": "citibike/trips/"}       # table name -> prefix in the clean bucket


def crawl(s3=None, con=None) -> dict:
    s3, con = s3 or client(), con or duckdb_connection()
    catalog = {"crawled_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "tables": {}}
    for table, prefix in TABLES.items():
        keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=CLEAN_BUCKET, Prefix=prefix)
                for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not keys:
            continue
        location = f"s3://{CLEAN_BUCKET}/{prefix}**/*.parquet"
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{location}', hive_partitioning = true)").fetchall()
        partitions = sorted({"/".join(p for p in k[len(prefix):].split("/")[:-1]) for k in keys})
        catalog["tables"][table] = {
            "location": location,
            "format": "parquet",
            "partition_keys": [kv.split("=")[0] for kv in partitions[0].split("/")],
            "partitions": partitions,
            "columns": [{"name": name, "type": dtype} for name, dtype, *_ in schema],
        }
        print(f"  {table}: {len(schema)} columns, {len(partitions)} partitions")
    s3.put_object(Bucket=CLEAN_BUCKET, Key=CATALOG_KEY, Body=json.dumps(catalog, indent=2).encode())
    return catalog


if __name__ == "__main__":
    crawl()
