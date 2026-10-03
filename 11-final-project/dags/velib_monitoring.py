"""Every 30 minutes: health checks on the pipeline's output (freshness, completeness, plausibility).
Each failed check raises an alert (monitoring.alerts + optional webhook). Critical failures also fail
the task, so the run shows red in the UI."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task
from velib_dag_utils import warehouse_connection


@dag(schedule="*/30 * * * *", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     tags=["velib", "final-project", "monitoring"], doc_md=__doc__)
def velib_monitoring():
    @task
    def health_checks() -> list[dict]:
        from velib import monitoring

        conn = warehouse_connection()
        try:
            monitoring.ensure_schema(conn)
            results = monitoring.run_checks(conn)
            for r in results:
                if not r.ok:
                    monitoring.raise_alert(conn, r.severity, "velib_monitoring", f"{r.name}: {r.detail}")
        finally:
            conn.close()
        critical = [r.name for r in results if not r.ok and r.severity == "critical"]
        if critical:
            raise RuntimeError(f"critical checks failed: {critical}")
        return [r.__dict__ for r in results]

    health_checks()


velib_monitoring()
