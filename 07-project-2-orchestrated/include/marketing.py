"""Landing and warehouse-load steps of the marketing pipeline (plain Python, called by the DAG)."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import requests

SOURCE_URL = "https://archive.ics.uci.edu/static/public/222/bank+marketing.zip"
INNER_ZIP, CSV_IN_ZIP = "bank-additional.zip", "bank-additional/bank-additional-full.csv"

COLUMNS = ["row_number", "age", "job", "marital", "education", "default", "housing", "loan", "contact",
           "month", "day_of_week", "duration", "campaign", "pdays", "previous", "poutcome", "emp_var_rate",
           "cons_price_idx", "cons_conf_idx", "euribor3m", "nr_employed", "year", "contact_month", "converted",
           "age_band", "previously_contacted", "days_since_previous", "partition_month"]

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.marketing_contacts (
    row_number bigint PRIMARY KEY, age int, job text, marital text, education text, "default" text,
    housing text, loan text, contact text, month text, day_of_week text, duration int, campaign int,
    pdays int, previous int, poutcome text, emp_var_rate numeric, cons_price_idx numeric,
    cons_conf_idx numeric, euribor3m numeric, nr_employed numeric, year int, contact_month date,
    converted boolean, age_band text, previously_contacted boolean, days_since_previous int,
    partition_month text, loaded_at timestamptz NOT NULL DEFAULT now()
)
"""


def land(dest_dir: Path) -> Path:
    """Download the archive (if not already landed) and extract the full CSV into the landing zone."""
    dest = dest_dir / "bank-additional-full.csv"
    if dest.exists():
        return dest
    dest_dir.mkdir(parents=True, exist_ok=True)
    r = requests.get(SOURCE_URL, timeout=120)
    r.raise_for_status()
    outer = zipfile.ZipFile(io.BytesIO(r.content))
    inner = zipfile.ZipFile(io.BytesIO(outer.read(INNER_ZIP)))
    dest.write_bytes(inner.read(CSV_IN_ZIP))
    return dest


def load_parquet_to_postgres(parquet_dir: Path, conn) -> int:
    """Full reload in one transaction (TRUNCATE + COPY): the table is small, and a full reload is
    the simplest idempotent strategy when the whole source is re-delivered every time."""
    import pyarrow.dataset as ds

    table = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive").to_table()
    df = table.to_pandas()[COLUMNS]
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    with conn.cursor() as cur:
        for statement in DDL.split(";"):
            if statement.strip():
                cur.execute(statement)
        cur.execute("TRUNCATE raw.marketing_contacts")
        cols = ", ".join(f'"{c}"' if c == "default" else c for c in COLUMNS)
        cur.copy_expert(f"COPY raw.marketing_contacts ({cols}) FROM STDIN WITH (FORMAT csv)", io.StringIO(buf.getvalue()))
    conn.commit()
    return len(df)
