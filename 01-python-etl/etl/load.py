"""Load: Parquet files (the "lake") and an idempotent upsert into Postgres (the "warehouse").

Idempotent means re-running the same day twice gives the same result, not duplicates. That is the
single most important property of a batch pipeline: it makes retries and backfills safe.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

DDL = """
CREATE TABLE IF NOT EXISTS daily_weather (
    city             text        NOT NULL,
    date             date        NOT NULL,
    temp_max_c       numeric(4, 1),
    temp_min_c       numeric(4, 1),
    precipitation_mm numeric(5, 1) CHECK (precipitation_mm >= 0),
    wind_max_kmh     numeric(5, 1) CHECK (wind_max_kmh >= 0),
    loaded_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (city, date),
    CHECK (temp_min_c <= temp_max_c)
);
"""


def write_parquet(df: pd.DataFrame, root: Path) -> list[Path]:
    """One file per city and month: data/city=Paris/month=2024-01/part.parquet (Hive-style partitions).

    Each run overwrites only the partitions it touched, so re-running a month replaces it cleanly.
    """
    written = []
    for (city, month), part in df.groupby([df["city"], df["date"].dt.strftime("%Y-%m")]):
        folder = root / f"city={city}" / f"month={month}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "part.parquet"
        part.drop(columns=["city"]).to_parquet(path, index=False)
        written.append(path)
    return written


def upsert_postgres(df: pd.DataFrame, dsn: str) -> int:
    """COPY the batch into a temp table, then INSERT ... ON CONFLICT DO UPDATE in one transaction.

    COPY is far faster than row-by-row INSERTs; the temp table + ON CONFLICT makes the load
    idempotent; the transaction makes it all-or-nothing.
    """
    import psycopg

    columns = ["city", "date", "temp_max_c", "temp_min_c", "precipitation_mm", "wind_max_kmh"]
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, date_format="%Y-%m-%d")
    with psycopg.connect(dsn) as con:  # one transaction: commits on success, rolls back on error
        con.execute(DDL)
        con.execute("CREATE TEMP TABLE staging (LIKE daily_weather INCLUDING DEFAULTS) ON COMMIT DROP")
        with con.cursor().copy(f"COPY staging ({', '.join(columns)}) FROM STDIN (FORMAT csv)") as copy:
            copy.write(buf.getvalue())
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns[2:])
        cur = con.execute(f"""
            INSERT INTO daily_weather ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM staging
            ON CONFLICT (city, date) DO UPDATE SET {updates}, loaded_at = now()
        """)
        return cur.rowcount
