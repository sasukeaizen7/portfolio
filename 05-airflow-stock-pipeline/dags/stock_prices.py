"""Daily stock prices: API -> raw JSON -> validated rows -> Postgres warehouse -> quality checks.

Airflow 3 TaskFlow API. Concepts on display:
  * a sensor (`@task.sensor`, reschedule mode) that waits for the API instead of failing;
  * dynamic task mapping: one `fetch` task instance per symbol, created at run time (`.expand`);
  * XComs: tasks pass small values (file paths, counts), never the data itself;
  * retries with exponential backoff, a run timeout, params with defaults overridable per run;
  * an Asset outlet: updating the table triggers the stock_report DAG (data-aware scheduling).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, PokeReturnValue, dag, get_current_context, task

DATA = Path("/opt/airflow/data")
DAILY_PRICES = Asset("postgres://warehouse/stocks/daily_prices")


def _warehouse_connection():
    # The "warehouse" Airflow connection comes from the AIRFLOW_CONN_WAREHOUSE env var (docker-compose.yml).
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="warehouse").get_conn()


@dag(
    schedule="0 23 * * 1-5",             # 23:00 UTC on weekdays, after the US close
    start_date=datetime(2026, 1, 1),
    catchup=False,                        # history is loaded with the lookback_days param instead
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    params={
        "symbols": Param(["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "MC.PA", "AIR.PA", "TTE.PA"], type="array"),
        "lookback_days": Param(10, type="integer", minimum=1, maximum=3650,
                               description="Days re-fetched each run (e.g. 730 for a first backfill)"),
    },
    tags=["stocks", "portfolio"],
    doc_md=__doc__,
)
def stock_prices():
    @task.sensor(poke_interval=60, timeout=1800, mode="reschedule")
    def api_available() -> PokeReturnValue:
        """Wait (without holding a worker slot) until the price API answers."""
        import requests

        from stock_pipeline.prices import CHART_URL, HEADERS

        try:
            ok = requests.get(CHART_URL.format(symbol="SPY"), params={"range": "1d"}, headers=HEADERS, timeout=10).ok
        except requests.RequestException:
            ok = False
        return PokeReturnValue(is_done=ok)

    @task
    def symbols() -> list[str]:
        return get_current_context()["params"]["symbols"]

    @task(max_active_tis_per_dagrun=3)    # be polite to the API: at most 3 symbols in parallel
    def fetch(symbol: str) -> str:
        from stock_pipeline.prices import fetch_chart, save_raw

        ctx = get_current_context()
        payload = fetch_chart(symbol, ctx["params"]["lookback_days"])
        folder = DATA / "raw" / "stocks" / ctx["run_id"].replace(":", "_")
        return str(save_raw(payload, folder, symbol))       # XCom: the path, not the data

    @task(outlets=[DAILY_PRICES])
    def load(raw_paths: list[str]) -> int:
        import json

        from stock_pipeline.prices import parse_chart, upsert

        rows = []
        for path in raw_paths:
            rows += parse_chart(json.loads(Path(path).read_text(encoding="utf-8")), Path(path).stem)
        conn = _warehouse_connection()
        try:
            return upsert(conn, rows)
        finally:
            conn.close()

    @task
    def quality_checks() -> dict:
        from stock_pipeline.prices import run_quality_checks

        conn = _warehouse_connection()
        try:
            return run_quality_checks(conn)       # raises -> the task fails -> the run is red
        finally:
            conn.close()

    paths = fetch.expand(symbol=symbols())
    api_available() >> paths
    load(paths) >> quality_checks()


stock_prices()
