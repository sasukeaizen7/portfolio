import os

import pandas as pd
import pytest
from etl.extract import CITIES
from etl.load import upsert_postgres, write_parquet
from etl.transform import to_frame


def test_parquet_partitions_roundtrip(tmp_path, paris_payload):
    df = to_frame(paris_payload, CITIES["paris"])
    files = write_parquet(df, tmp_path)
    assert [p.relative_to(tmp_path).as_posix() for p in files] == ["city=Paris/month=2024-01/part.parquet"]
    back = pd.read_parquet(tmp_path)  # pyarrow turns the folder names back into columns
    assert len(back) == 3 and set(back["city"].astype(str)) == {"Paris"}


def test_rewriting_a_partition_replaces_it(tmp_path, paris_payload):
    df = to_frame(paris_payload, CITIES["paris"])
    write_parquet(df, tmp_path)
    write_parquet(df, tmp_path)
    assert len(pd.read_parquet(tmp_path)) == 3


@pytest.mark.skipif(not os.environ.get("PG_DSN"), reason="needs a Postgres in $PG_DSN")
def test_upsert_is_idempotent(paris_payload):
    import psycopg

    dsn = os.environ["PG_DSN"]
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS daily_weather")
    df = to_frame(paris_payload, CITIES["paris"])
    assert upsert_postgres(df, dsn) == 3
    df.loc[0, "temp_max_c"] = 10.5            # a corrected value arrives on re-run
    assert upsert_postgres(df, dsn) == 3      # same 3 rows updated, no duplicates
    with psycopg.connect(dsn) as con:
        rows = con.execute("SELECT count(*), max(temp_max_c) FILTER (WHERE date = '2024-01-01') FROM daily_weather").fetchone()
    assert rows[0] == 3 and float(rows[1]) == 10.5
