"""S3 access for the lake. The same code talks to AWS S3, SeaweedFS (docker compose) or moto (tests):
only the endpoint and credentials change, read from the environment like an AWS SDK would.

    S3_ENDPOINT=http://localhost:8333  AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
import duckdb

RAW_BUCKET = os.environ.get("LAKE_RAW_BUCKET", "lake-raw")
CLEAN_BUCKET = os.environ.get("LAKE_CLEAN_BUCKET", "lake-clean")
RESULTS_BUCKET = os.environ.get("LAKE_RESULTS_BUCKET", "lake-results")


def endpoint() -> str | None:
    return os.environ.get("S3_ENDPOINT")          # None -> real AWS


def client(access_key: str | None = None, secret_key: str | None = None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint(),
        aws_access_key_id=access_key or os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def duckdb_connection(access_key: str | None = None, secret_key: str | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB session that reads and writes s3:// paths (DuckDB is our stand-in for Athena)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    ep = endpoint()
    params = {
        "TYPE": "s3",
        "KEY_ID": access_key or os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "SECRET": secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "REGION": os.environ.get("AWS_REGION", "us-east-1"),
    }
    if ep:
        u = urlparse(ep)
        params.update(ENDPOINT=u.netloc, URL_STYLE="path", USE_SSL="true" if u.scheme == "https" else "false")
    body = ", ".join(f"{k} '{v}'" if k != "TYPE" else f"{k} {v}" for k, v in params.items())
    con.execute(f"CREATE OR REPLACE SECRET lake ({body})")
    return con
