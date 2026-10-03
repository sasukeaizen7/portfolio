"""Landing: download the source once and turn it into a clean, typed Parquet "feed".

The source is an Excel workbook with one sheet per year. Excel is a poor interchange format (slow to
parse, types guessed per cell), so it is converted once, with DuckDB, into a single Parquet file
that the daily batches read. Two source quirks are fixed here, and only here:
  * the two sheets overlap on 1-9 December 2010 (those lines appear twice);
  * dates are Excel serial numbers (days since 1899-12-30).
Each line gets a stable `line_id` (its position in the source), so every warehouse row can be traced
back to the exact source line.
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import duckdb
import requests

log = logging.getLogger(__name__)

SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
WORKBOOK = "online_retail_II.xlsx"
SHEETS = ("Year 2009-2010", "Year 2010-2011")


def download(data_dir: Path) -> Path:
    xlsx = data_dir / WORKBOOK
    if xlsx.exists():
        return xlsx
    data_dir.mkdir(parents=True, exist_ok=True)
    archive = data_dir / "online_retail_ii.zip"
    log.info("downloading %s", SOURCE_URL)
    with requests.get(SOURCE_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(archive, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(archive) as z:
        z.extract(WORKBOOK, data_dir)
    archive.unlink()
    return xlsx


def build_feed(xlsx: Path, feed: Path) -> Path:
    """Excel -> one typed, de-overlapped Parquet file, sorted by time."""
    if feed.exists():
        return feed
    con = duckdb.connect()
    con.execute("INSTALL excel; LOAD excel;")
    union = " UNION ALL BY NAME ".join(
        f"SELECT '{s}' AS sheet, row_number() OVER () AS row_in_sheet, * "
        f"FROM read_xlsx('{xlsx.as_posix()}', sheet = '{s}', all_varchar = true)" for s in SHEETS)
    con.execute(f"""
        COPY (
            WITH raw AS ({union}),
            typed AS (
                SELECT sheet, row_in_sheet,
                       Invoice                                                           AS invoice,
                       StockCode                                                         AS stock_code,
                       nullif(trim(Description), '')                                     AS description,
                       CAST(Quantity AS INTEGER)                                         AS quantity,
                       TIMESTAMP '1899-12-30'
                         + to_milliseconds(CAST(round(CAST(InvoiceDate AS DOUBLE) * 86400000) AS BIGINT)) AS invoiced_at,
                       round(CAST(Price AS DOUBLE), 2)                                   AS unit_price,
                       CAST(CAST("Customer ID" AS DOUBLE) AS INTEGER)                    AS customer_id,
                       Country                                                           AS country
                FROM raw
            ),
            second_sheet_start AS (SELECT min(invoiced_at) AS ts FROM typed WHERE sheet = '{SHEETS[1]}')
            SELECT row_number() OVER (ORDER BY sheet, row_in_sheet) AS line_id,
                   invoice, stock_code, description, quantity, invoiced_at, unit_price, customer_id, country
            FROM typed
            WHERE NOT (sheet = '{SHEETS[0]}' AND invoiced_at >= (SELECT ts FROM second_sheet_start))
            ORDER BY invoiced_at, line_id
        ) TO '{feed.as_posix()}' (FORMAT parquet)
    """)
    n, first, last = con.execute(f"SELECT count(*), min(invoiced_at), max(invoiced_at) FROM '{feed.as_posix()}'").fetchone()
    log.info("feed ready: %s lines, %s -> %s", f"{n:,}", first, last)
    return feed


class Feed:
    """The landing file loaded once into an in-memory DuckDB table, then served one day at a time
    (re-scanning the Parquet file for every daily batch would dominate the run time)."""

    def __init__(self, path: Path):
        self.con = duckdb.connect()
        self.con.execute(f"CREATE TABLE feed AS SELECT * FROM '{path.as_posix()}'")

    def trading_days(self) -> list:
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT CAST(invoiced_at AS DATE) FROM feed ORDER BY 1").fetchall()]

    def lines_for_day(self, day) -> list[tuple]:
        """The 'daily file' of one trading day, as the shop's system would export it."""
        return self.con.execute(
            "SELECT line_id, invoice, stock_code, description, quantity, invoiced_at, unit_price, customer_id, country "
            "FROM feed WHERE CAST(invoiced_at AS DATE) = ? ORDER BY line_id", [day]).fetchall()
