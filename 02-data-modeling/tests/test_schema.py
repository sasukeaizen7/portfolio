"""Runs the schema and the SCD 2 merge against a real PostgreSQL (database de_model_test)."""

import os
from datetime import date
from pathlib import Path

import psycopg
import pytest

SQL = Path(__file__).resolve().parents[1] / "sql"
ADMIN_DSN = os.environ.get("PG_DSN")
pytestmark = pytest.mark.skipif(not ADMIN_DSN, reason="needs a Postgres in $PG_DSN")


@pytest.fixture
def db():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute("DROP DATABASE IF EXISTS de_model_test WITH (FORCE)")
        admin.execute("CREATE DATABASE de_model_test")
    dsn = ADMIN_DSN.rsplit("/", 1)[0] + "/de_model_test"
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute((SQL / "01_schema.sql").read_text(encoding="utf-8"))
        con.execute((SQL / "02_seed_dimensions.sql").read_text(encoding="utf-8"))
        con.execute("CREATE SCHEMA stage; CREATE TABLE stage.customer_snapshot (customer_id int, country text, as_of date)")
        yield con


def merge(con, as_of: date, rows: list[tuple[int, str]]) -> None:
    with con.transaction():
        con.execute("TRUNCATE stage.customer_snapshot")
        for cid, country in rows:
            con.execute("INSERT INTO stage.customer_snapshot VALUES (%s, %s, %s)", (cid, country, as_of))
        con.execute((SQL / "03_scd2_merge.sql").read_text(encoding="utf-8"))


def versions(con, cid: int):
    return con.execute("""
        SELECT co.country, cu.valid_from, cu.valid_to, cu.is_current
        FROM dw.dim_customer cu JOIN dw.dim_country co USING (country_key)
        WHERE customer_id = %s ORDER BY valid_from""", (cid,)).fetchall()


def test_calendar_is_complete(db):
    n, first, last, weekends = db.execute(
        "SELECT count(*), min(date), max(date), count(*) FILTER (WHERE is_weekend) FROM dw.dim_date").fetchone()
    assert (first, last) == (date(2009, 12, 1), date(2011, 12, 31))
    assert n == (last - first).days + 1 and weekends > 0


def test_scd2_history(db):
    merge(db, date(2011, 1, 10), [(1, "France"), (2, "United Kingdom")])
    merge(db, date(2011, 3, 1), [(1, "Germany"), (2, "United Kingdom")])   # 1 moves, 2 doesn't
    assert versions(db, 1) == [
        ("France", date(2011, 1, 10), date(2011, 2, 28), False),
        ("Germany", date(2011, 3, 1), date(9999, 12, 31), True),
    ]
    assert len(versions(db, 2)) == 1


def test_scd2_merge_is_idempotent(db):
    for _ in range(3):
        merge(db, date(2011, 3, 1), [(1, "Germany")])
    assert len(versions(db, 1)) == 1


def test_same_day_correction_overwrites(db):
    merge(db, date(2011, 1, 10), [(1, "France")])
    merge(db, date(2011, 1, 10), [(1, "Spain")])     # corrected the same day
    assert versions(db, 1) == [("Spain", date(2011, 1, 10), date(9999, 12, 31), True)]


def test_regions_and_one_current_version(db):
    merge(db, date(2011, 1, 10), [(1, "EIRE"), (2, "Japan")])
    assert dict(db.execute("SELECT country, region FROM dw.dim_country").fetchall()) == {
        "EIRE": "Europe", "Japan": "Rest of world"}
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute("INSERT INTO dw.dim_customer (customer_id, country_key, valid_from) "
                   "SELECT 1, country_key, '2011-05-01' FROM dw.dim_country LIMIT 1")


def test_fact_rejects_a_sale_with_negative_quantity(db):
    db.execute("INSERT INTO dw.dim_country (country, region) VALUES ('France', 'Europe')")
    db.execute("INSERT INTO dw.dim_customer (customer_id, country_key, valid_from) VALUES (1, 1, '2011-01-01')")
    db.execute("INSERT INTO dw.dim_product (stock_code, description, is_product) VALUES ('85123A', 'HEART', true)")
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("""INSERT INTO dw.fact_sales VALUES (1, '536365', 20110110, 1, 1, '2011-01-10 08:26',
                                                         -6, 2.55, -15.30, 'sale')""")
