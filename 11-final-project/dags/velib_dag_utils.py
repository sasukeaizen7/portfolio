"""Shared by the DAG files: the warehouse connection and the failure callback that raises alerts.
(Not a DAG: the DAG processor ignores files without one.)"""

from __future__ import annotations


def warehouse_connection():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="warehouse").get_conn()


def alert_on_failure(context) -> None:
    """on_failure_callback: every task that fails for good (after its retries) becomes an alert."""
    from velib import monitoring

    ti = context["ti"]
    conn = warehouse_connection()
    try:
        monitoring.ensure_schema(conn)
        monitoring.raise_alert(conn, "critical", f"{ti.dag_id}.{ti.task_id}",
                               f"task failed after retries (run {context['run_id']}): {context.get('exception')}")
    finally:
        conn.close()
