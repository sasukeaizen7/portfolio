"""Every 10 minutes: Vélib' API -> S3 raw zone -> warehouse raw tables. Publishes the raw Asset.

`capture` and `load` are separate tasks on purpose: if the warehouse is down, snapshots keep landing
in S3, and the next successful `load` catches up on everything not yet loaded.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task
from velib_dag_utils import alert_on_failure, warehouse_connection

RAW = Asset("postgres://warehouse/raw/velib_station_status")


@dag(
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=9),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "on_failure_callback": alert_on_failure},
    tags=["velib", "final-project"],
    doc_md=__doc__,
)
def velib_ingest():
    @task
    def capture() -> str:
        from velib import lake, pipeline

        return pipeline.capture(lake.client())

    @task(outlets=[RAW])
    def load() -> dict:
        from velib import lake, pipeline

        conn = warehouse_connection()
        try:
            return pipeline.load_new(lake.client(), conn)
        finally:
            conn.close()

    capture() >> load()


velib_ingest()
