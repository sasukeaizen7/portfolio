"""The "Athena" step: register every catalog table as a view over its S3 location, run the named
queries in sql/queries.sql, save each result to s3://lake-results/ and results/<name>.csv.

Runs with the ANALYST credentials (read-only on the clean zone, write on results): the access
control is enforced by the object store, not by this code (see docker/policies/).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .s3 import CLEAN_BUCKET, RESULTS_BUCKET, client, duckdb_connection

HERE = Path(__file__).resolve().parents[1]
NAME_RE = re.compile(r"^--\s*name:\s*([a-z0-9_]+)\s*$", re.MULTILINE)


def analyst_credentials() -> tuple[str | None, str | None]:
    return os.environ.get("ANALYST_ACCESS_KEY"), os.environ.get("ANALYST_SECRET_KEY")


def register_catalog(con, s3) -> list[str]:
    catalog = json.loads(s3.get_object(Bucket=CLEAN_BUCKET, Key="_catalog/catalog.json")["Body"].read())
    for table, meta in catalog["tables"].items():
        con.execute(f"CREATE OR REPLACE VIEW {table} AS "
                    f"SELECT * FROM read_parquet('{meta['location']}', hive_partitioning = true)")
    return list(catalog["tables"])


def run_queries(sql_text: str, out_dir: Path | None = None) -> dict:
    key, secret = analyst_credentials()
    s3, con = client(key, secret), duckdb_connection(key, secret)
    register_catalog(con, s3)
    parts = NAME_RE.split(sql_text)
    results = {}
    for i in range(1, len(parts), 2):
        name, query = parts[i], parts[i + 1].strip().rstrip(";")
        df = con.execute(query).df()
        results[name] = df
        s3.put_object(Bucket=RESULTS_BUCKET, Key=f"queries/{name}.csv", Body=df.to_csv(index=False).encode())
        if out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
            df.to_csv(out_dir / f"{name}.csv", index=False)
        print(f"  {name}: {len(df)} rows")
    return results


if __name__ == "__main__":
    run_queries((HERE / "sql" / "queries.sql").read_text(encoding="utf-8"), HERE / "results")
