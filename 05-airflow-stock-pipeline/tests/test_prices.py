import copy
import json
import os
from datetime import date
from pathlib import Path

import pytest

from stock_pipeline.prices import PriceDataError, parse_chart, run_quality_checks, upsert

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "aapl_2025-01-01_08.json").read_text(encoding="utf-8"))
SQL = Path(__file__).resolve().parents[1] / "sql"


def test_parse_real_response():
    rows = parse_chart(FIXTURE, "AAPL")
    # 2-8 January 2025: 4 trading days (New Year's Day closed, weekend skipped), in exchange-local dates.
    assert [r.trade_date for r in rows] == [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6), date(2025, 1, 7)]
    assert all(r.low <= r.close <= r.high for r in rows) and rows[0].volume > 0


def test_missing_bars_are_skipped_not_invented():
    p = copy.deepcopy(FIXTURE)
    p["chart"]["result"][0]["indicators"]["quote"][0]["close"][1] = None
    assert len(parse_chart(p, "AAPL")) == 3


@pytest.mark.parametrize("field, value, message", [
    ("low", 10_000.0, "outside the low-high range"),
    ("open", -1.0, "non-positive price"),
    ("volume", -5, "negative volume"),
])
def test_invalid_bars_are_refused(field, value, message):
    p = copy.deepcopy(FIXTURE)
    p["chart"]["result"][0]["indicators"]["quote"][0][field][0] = value
    with pytest.raises(PriceDataError, match=message):
        parse_chart(p, "AAPL")


def test_api_errors_are_reported():
    with pytest.raises(PriceDataError, match="API error"):
        parse_chart({"chart": {"result": None, "error": {"code": "Not Found"}}}, "NOPE")


@pytest.mark.skipif(not os.environ.get("PG_DSN"), reason="needs a Postgres in $PG_DSN")
def test_upsert_is_idempotent_and_report_builds():
    import psycopg

    with psycopg.connect(os.environ["PG_DSN"]) as conn:
        conn.execute("DROP SCHEMA IF EXISTS stocks CASCADE")
        conn.commit()
        rows = parse_chart(FIXTURE, "AAPL")
        assert upsert(conn, rows) == 4
        assert upsert(conn, rows) == 4                       # same window again: no duplicates
        assert conn.execute("SELECT count(*) FROM stocks.daily_prices").fetchone()[0] == 4
        assert run_quality_checks(conn)["no weekend trading dates"] == 0
        conn.execute("UPDATE stocks.daily_prices SET trade_date = current_date + 3 WHERE trade_date = '2025-01-02'")
        with pytest.raises(PriceDataError, match="no future trading dates"):
            run_quality_checks(conn)
        conn.rollback()
        conn.execute((SQL / "symbol_report.sql").read_text(encoding="utf-8"))
        conn.commit()
        symbol, first, last, ret, dd = conn.execute(
            "SELECT symbol, first_date, last_date, return_pct, max_drawdown_pct FROM stocks.symbol_report").fetchone()
        closes = [r.adj_close for r in rows]
        assert (symbol, first, last) == ("AAPL", date(2025, 1, 2), date(2025, 1, 7))
        assert float(ret) == pytest.approx(100 * (closes[-1] / closes[0] - 1), abs=0.01)
        assert float(dd) <= 0
