"""Daily batch pipeline: landing Parquet -> raw.sales_lines -> star schema (dw.*), with audit and checks.

    python -m ingest.pipeline setup                       # schemas, tables, calendar
    python -m ingest.pipeline run --date 2011-11-03       # one day
    python -m ingest.pipeline backfill [--from D] [--to D] # every trading day in order

Each batch is ONE transaction: delete + insert the day's raw lines, SCD 2 merge, dimensions, facts,
quality checks. If any check fails, everything rolls back and the failure is recorded in
audit.load_runs / audit.dq_results (written outside the rolled-back transaction).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import psycopg

from . import landing

log = logging.getLogger("ingest")

HERE = Path(__file__).resolve().parents[1]
SQL = HERE / "sql"
MODEL_SQL = Path(os.environ.get("MODEL_SQL_DIR", HERE.parent / "02-data-modeling" / "sql"))
DATA = Path(os.environ.get("DATA_DIR", HERE / "data"))
RAW_COLUMNS = ["line_id", "invoice", "stock_code", "description", "quantity", "invoiced_at", "unit_price",
               "customer_id", "country"]


class QualityCheckFailed(RuntimeError):
    def __init__(self, failed: list[tuple]):
        super().__init__("; ".join(f"{name} ({observed})" for name, _, observed in failed))
        self.failed = failed


def sql(name: str, folder: Path = SQL) -> str:
    return (folder / name).read_text(encoding="utf-8")


def execute_script(con: psycopg.Connection, text: str, params: dict) -> list[tuple]:
    """Run a parameterized multi-statement file one statement at a time (a prepared statement holds
    only one). Returns the rows of the last statement. Our files never put ';' inside a literal."""
    statements = [s for s in (part.strip() for part in text.split(";")) if s and not _only_comments(s)]
    cur = None
    for statement in statements:
        cur = con.execute(statement, params)
    return cur.fetchall() if cur is not None and cur.description else []


def _only_comments(statement: str) -> bool:
    return all(not line.strip() or line.strip().startswith("--") for line in statement.splitlines())


def setup(con: psycopg.Connection) -> None:
    with con.transaction():
        con.execute(sql("01_schema.sql", MODEL_SQL))
        con.execute(sql("02_seed_dimensions.sql", MODEL_SQL))
        con.execute(sql("00_pipeline_tables.sql"))
    log.info("schema ready")


def feed() -> landing.Feed:
    return landing.Feed(landing.build_feed(landing.download(DATA), DATA / "online_retail_feed.parquet"))


def run_batch(con: psycopg.Connection, day: date, lines: list[tuple]) -> dict:
    started = time.perf_counter()
    run_id = con.execute("INSERT INTO audit.load_runs (batch_date, started_at, status) VALUES (%s, now(), 'running') "
                         "RETURNING run_id", (day,)).fetchone()[0]
    params = {"batch_date": day}
    checks: list[tuple] = []
    try:
        with con.transaction():
            con.execute("DELETE FROM raw.sales_lines WHERE batch_date = %s", (day,))
            with con.cursor().copy(f"COPY raw.sales_lines (batch_date, {', '.join(RAW_COLUMNS)}) FROM STDIN") as cp:
                for line in lines:
                    cp.write_row((day, *line))
            execute_script(con, sql("10_transform_batch.sql"), params)
            con.execute(sql("03_scd2_merge.sql", MODEL_SQL))
            execute_script(con, sql("20_load_facts.sql"), params)
            checks = execute_script(con, sql("30_quality_checks.sql"), params)
            failed = [c for c in checks if not c[1]]
            if failed:
                raise QualityCheckFailed(failed)
            fact_lines = con.execute("SELECT count(*) FROM dw.fact_sales WHERE date_key = %s",
                                     (int(day.strftime("%Y%m%d")),)).fetchone()[0]
    except Exception as e:
        con.execute("UPDATE audit.load_runs SET status = 'failed', finished_at = now(), error = %s WHERE run_id = %s",
                    (str(e)[:2000], run_id))
        _record_checks(con, run_id, checks)
        raise
    con.execute("UPDATE audit.load_runs SET status = 'success', finished_at = now(), raw_lines = %s, "
                "fact_lines = %s, rejected = %s WHERE run_id = %s",
                (len(lines), fact_lines, len(lines) - fact_lines, run_id))
    _record_checks(con, run_id, checks)
    return {"day": day, "raw": len(lines), "fact": fact_lines, "seconds": time.perf_counter() - started}


def _record_checks(con: psycopg.Connection, run_id: int, checks: list[tuple]) -> None:
    for name, passed, observed in checks:
        con.execute("INSERT INTO audit.dq_results VALUES (%s, %s, %s, %s)", (run_id, name, passed, observed))


def backfill(con: psycopg.Connection, start: date | None, end: date | None) -> None:
    source = feed()
    days = [d for d in source.trading_days() if (not start or d >= start) and (not end or d <= end)]
    log.info("backfilling %d trading days", len(days))
    t0 = time.perf_counter()
    for i, day in enumerate(days, 1):
        r = run_batch(con, day, source.lines_for_day(day))
        if i % 50 == 0 or i == len(days):
            log.info("%d/%d days loaded (last: %s, %d raw -> %d fact lines), %.0fs elapsed",
                     i, len(days), r["day"], r["raw"], r["fact"], time.perf_counter() - t0)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup")
    r = sub.add_parser("run")
    r.add_argument("--date", type=date.fromisoformat, required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--from", dest="start", type=date.fromisoformat)
    b.add_argument("--to", dest="end", type=date.fromisoformat)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        p.error("set PG_DSN, e.g. postgresql://de:de@localhost:5435/de")
    with psycopg.connect(dsn, autocommit=True) as con:
        setup(con)
        if args.cmd == "run":
            print(run_batch(con, args.date, feed().lines_for_day(args.date)))
        elif args.cmd == "backfill":
            backfill(con, args.start, args.end)
    return 0


if __name__ == "__main__":
    sys.exit(main())
