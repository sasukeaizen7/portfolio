"""Per-symbol report (return, volatility, max drawdown, trend), refreshed whenever prices are loaded.

Scheduled on an Asset, not on a clock: it runs right after stock_prices updates the table, and never
on a day when the load failed. The SQL lives in sql/symbol_report.sql.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, dag

@dag(
    schedule=[Asset("postgres://warehouse/stocks/daily_prices")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    tags=["stocks", "portfolio"],
    doc_md=__doc__,
)
def stock_report():
    SQLExecuteQueryOperator(task_id="refresh_symbol_report", conn_id="warehouse", sql="symbol_report.sql")


stock_report()
