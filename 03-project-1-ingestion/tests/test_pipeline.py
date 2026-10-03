"""End-to-end tests of the batch pipeline on small hand-made batches (database de_ingest_test)."""

import os
from datetime import date, datetime

import psycopg
import pytest

from ingest import pipeline

ADMIN_DSN = os.environ.get("PG_DSN")
pytestmark = pytest.mark.skipif(not ADMIN_DSN, reason="needs a Postgres in $PG_DSN")


def line(line_id, day, customer, country, qty=2, price=1.25, code="85123A", desc="WHITE HEART", invoice=None):
    return (line_id, invoice or f"5{line_id:05d}", code, desc, qty, datetime.fromisoformat(f"{day} 10:00"),
            price, customer, country)


@pytest.fixture
def con():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute("DROP DATABASE IF EXISTS de_ingest_test WITH (FORCE)")
        admin.execute("CREATE DATABASE de_ingest_test")
    with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + "/de_ingest_test", autocommit=True) as c:
        pipeline.setup(c)
        yield c


def one(con, query, *args):
    return con.execute(query, args).fetchone()


DAY1, DAY2 = date(2011, 1, 10), date(2011, 3, 1)
BATCH1 = [line(1, DAY1, 100, "France"), line(2, DAY1, None, "France"),            # identified + guest
          line(3, DAY1, 100, "France", qty=-2, invoice="C500003"),                 # cancellation
          line(4, DAY1, None, "France", qty=1, price=-50.0, code="B", desc="Adjust bad debt")]  # rejected
BATCH2 = [line(5, DAY2, 100, "Germany", desc="WHITE HANGING HEART")]               # moved + renamed


def test_batches_build_the_star(con):
    r1 = pipeline.run_batch(con, DAY1, BATCH1)
    pipeline.run_batch(con, DAY2, BATCH2)
    assert (r1["raw"], r1["fact"]) == (4, 3)                                     # one line rejected
    assert one(con, "SELECT count(*), sum(revenue) FROM dw.fact_sales") == (4, 2.50 + 2.50 - 2.50 + 2.50)
    # SCD 2: day-1 sales stay on the France version, day-2 sale on the Germany version.
    rows = con.execute("""SELECT f.sales_line_id, co.country FROM dw.fact_sales f
                          JOIN dw.dim_customer cu USING (customer_key) JOIN dw.dim_country co USING (country_key)
                          WHERE cu.customer_id = 100 ORDER BY 1""").fetchall()
    assert rows == [(1, "France"), (3, "France"), (5, "Germany")]
    # Guest line -> the France guest member; SCD 1 product description overwritten.
    assert one(con, "SELECT cu.customer_id FROM dw.fact_sales f JOIN dw.dim_customer cu USING (customer_key) "
                    "WHERE sales_line_id = 2") == (None,)
    assert one(con, "SELECT description FROM dw.dim_product WHERE stock_code = '85123A'") == ("WHITE HANGING HEART",)
    assert one(con, "SELECT count(*) FROM dw.fact_sales WHERE line_type = 'cancellation'") == (1,)


def test_rerunning_an_old_batch_is_idempotent(con):
    pipeline.run_batch(con, DAY1, BATCH1)
    pipeline.run_batch(con, DAY2, BATCH2)
    before = con.execute("SELECT * FROM dw.fact_sales ORDER BY 1").fetchall()
    pipeline.run_batch(con, DAY1, BATCH1)                     # re-run day 1 after day 2
    assert con.execute("SELECT * FROM dw.fact_sales ORDER BY 1").fetchall() == before
    assert one(con, "SELECT count(*) FROM dw.dim_customer WHERE customer_id = 100") == (2,)


def test_every_run_is_audited_with_checks(con):
    pipeline.run_batch(con, DAY1, BATCH1)
    status, raw, fact, rejected = one(con, "SELECT status, raw_lines, fact_lines, rejected FROM audit.load_runs")
    assert (status, raw, fact, rejected) == ("success", 4, 3, 1)
    assert one(con, "SELECT count(*), bool_and(passed) FROM audit.dq_results") == (6, True)


def test_a_failing_batch_rolls_back_and_is_recorded(con):
    pipeline.run_batch(con, DAY1, BATCH1)
    bad = [line(6, DAY2, 200, None)]                          # country missing: violates raw NOT NULL
    with pytest.raises(psycopg.errors.NotNullViolation):
        pipeline.run_batch(con, DAY2, bad)
    assert one(con, "SELECT count(*) FROM raw.sales_lines WHERE batch_date = %s", DAY2) == (0,)
    assert one(con, "SELECT status FROM audit.load_runs ORDER BY run_id DESC LIMIT 1") == ("failed",)


def test_quality_check_failure_blocks_the_batch(con, monkeypatch):
    # Sabotage the fact load (skip guest lines) so the reconciliation check must catch it.
    original = pipeline.sql
    monkeypatch.setattr(pipeline, "sql", lambda name, folder=pipeline.SQL: original(name, folder).replace(
        "AND r.unit_price >= 0", "AND r.unit_price >= 0 AND r.customer_id IS NOT NULL")
        if name == "20_load_facts.sql" else original(name, folder))
    with pytest.raises(pipeline.QualityCheckFailed, match="every raw line is loaded or rejected"):
        pipeline.run_batch(con, DAY1, BATCH1)
    assert one(con, "SELECT count(*) FROM dw.fact_sales") == (0,)
    assert one(con, "SELECT count(*) FROM audit.dq_results WHERE NOT passed") == (2,)
