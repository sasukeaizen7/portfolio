import os

import pandas as pd
import pytest

from marketing import COLUMNS, load_parquet_to_postgres

pytestmark = pytest.mark.skipif(not os.environ.get("PG_DSN"), reason="needs a Postgres in $PG_DSN")


def test_full_reload_is_idempotent(tmp_path):
    psycopg2 = pytest.importorskip("psycopg2")
    row = {c: None for c in COLUMNS}
    row.update(row_number=0, age=30, month="may", campaign=1, pdays=999, year=2008,
               contact_month=pd.Timestamp("2008-05-01").date(), converted=True, partition_month="2008-05")
    pd.DataFrame([row, {**row, "row_number": 1}]).to_parquet(tmp_path / "out", partition_cols=["partition_month"], index=False)
    conn = psycopg2.connect(os.environ["PG_DSN"])
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS raw.marketing_contacts")
        conn.commit()
        assert load_parquet_to_postgres(tmp_path / "out", conn) == 2
        assert load_parquet_to_postgres(tmp_path / "out", conn) == 2
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), bool_and(converted) FROM raw.marketing_contacts")
            assert cur.fetchone() == (2, True)
    finally:
        conn.close()
