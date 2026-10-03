"""Daily stock prices: fetch (Yahoo Finance chart API), parse/validate, load (idempotent upsert).

Plain Python with no Airflow import, so it can be unit-tested anywhere. The DAG in dags/ only wires
these functions into tasks. Yahoo's chart endpoint is free and keyless but unofficial: it can change
or rate-limit, which is exactly why the DAG checks it with a sensor first and retries tasks.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0 (de100-portfolio stock pipeline)"}


@dataclass(frozen=True)
class PriceRow:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int


class PriceDataError(ValueError):
    pass


def fetch_chart(symbol: str, lookback_days: int, *, session=None, attempts: int = 3, backoff_s: float = 2.0) -> dict:
    """Raw JSON for the last `lookback_days` days of daily bars."""
    http = session or requests.Session()
    now = int(time.time())
    params = {"period1": now - lookback_days * 86400, "period2": now, "interval": "1d", "events": "div,splits"}
    for attempt in range(1, attempts + 1):
        r = http.get(CHART_URL.format(symbol=symbol), params=params, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code not in (429, 500, 502, 503, 504) or attempt == attempts:
            raise PriceDataError(f"{symbol}: HTTP {r.status_code}")
        time.sleep(backoff_s * 2 ** (attempt - 1))
    raise AssertionError("unreachable")


def save_raw(payload: dict, folder: Path, symbol: str) -> Path:
    """Keep the untouched API response: the raw zone lets you re-process without calling the API again."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{symbol}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def parse_chart(payload: dict, symbol: str) -> list[PriceRow]:
    """API JSON -> validated rows. Bars with missing prices (holidays, halts) are skipped, not invented."""
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise PriceDataError(f"{symbol}: API error {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise PriceDataError(f"{symbol}: empty result")
    res = results[0]
    stamps = res.get("timestamp") or []
    quote = res["indicators"]["quote"][0]
    adj = (res["indicators"].get("adjclose") or [{}])[0].get("adjclose") or quote["close"]
    tz = ZoneInfo(res["meta"].get("exchangeTimezoneName", "UTC"))
    rows = []
    for i, ts in enumerate(stamps):
        values = [quote["open"][i], quote["high"][i], quote["low"][i], quote["close"][i], adj[i], quote["volume"][i]]
        if any(v is None for v in values):
            continue
        o, h, lo, c, a, v = values
        row = PriceRow(symbol, datetime.fromtimestamp(ts, UTC).astimezone(tz).date(),
                       round(o, 4), round(h, 4), round(lo, 4), round(c, 4), round(a, 4), int(v))
        validate(row)
        rows.append(row)
    if len({r.trade_date for r in rows}) != len(rows):
        raise PriceDataError(f"{symbol}: duplicate trading days")
    return rows


def validate(row: PriceRow) -> None:
    if min(row.open, row.high, row.low, row.close) <= 0:
        raise PriceDataError(f"{row.symbol} {row.trade_date}: non-positive price")
    if not (row.low <= min(row.open, row.close) and max(row.open, row.close) <= row.high):
        raise PriceDataError(f"{row.symbol} {row.trade_date}: open/close outside the low-high range")
    if row.volume < 0:
        raise PriceDataError(f"{row.symbol} {row.trade_date}: negative volume")


DDL = """
CREATE SCHEMA IF NOT EXISTS stocks;
CREATE TABLE IF NOT EXISTS stocks.daily_prices (
    symbol     text           NOT NULL,
    trade_date date           NOT NULL,
    open       numeric(14, 4) NOT NULL,
    high       numeric(14, 4) NOT NULL,
    low        numeric(14, 4) NOT NULL,
    close      numeric(14, 4) NOT NULL,
    adj_close  numeric(14, 4) NOT NULL,
    volume     bigint         NOT NULL CHECK (volume >= 0),
    loaded_at  timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date),
    CHECK (low <= LEAST(open, close) AND GREATEST(open, close) <= high)
)
"""

UPSERT = """
INSERT INTO stocks.daily_prices (symbol, trade_date, open, high, low, close, adj_close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close, volume = EXCLUDED.volume, loaded_at = now()
"""


def upsert(conn, rows: list[PriceRow]) -> int:
    """Idempotent load through any DB-API connection (psycopg2 from an Airflow hook, or psycopg 3).

    Every run re-fetches a window of recent days and upserts it, so a failed or late run heals itself
    on the next one, and corrected prices (adjustments) overwrite the old ones.
    """
    with conn.cursor() as cur:
        for statement in DDL.split(";"):
            if statement.strip():
                cur.execute(statement)
        cur.executemany(UPSERT, [(r.symbol, r.trade_date, r.open, r.high, r.low, r.close, r.adj_close, r.volume)
                                 for r in rows])
    conn.commit()
    return len(rows)


QUALITY_CHECKS = {
    # name -> SQL returning the number of offending rows (must be 0)
    "no future trading dates": "SELECT count(*) FROM stocks.daily_prices WHERE trade_date > current_date",
    "no weekend trading dates": "SELECT count(*) FROM stocks.daily_prices WHERE extract(isodow FROM trade_date) > 5",
    "daily move under 50%": """
        SELECT count(*) FROM (
            SELECT close / lag(close) OVER (PARTITION BY symbol ORDER BY trade_date) - 1 AS r
            FROM stocks.daily_prices) x
        WHERE abs(r) > 0.5""",
}


def run_quality_checks(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        results = {}
        for name, query in QUALITY_CHECKS.items():
            cur.execute(query)
            results[name] = cur.fetchone()[0]
    failed = {k: v for k, v in results.items() if v}
    if failed:
        raise PriceDataError(f"quality checks failed: {failed}")
    return results
