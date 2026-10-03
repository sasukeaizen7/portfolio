"""dbt build (models + tests) each time new raw data lands: scheduled on the raw Asset, not on a clock.
A failing dbt test fails the task, which raises an alert through the failure callback."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag
from velib_dag_utils import alert_on_failure

DBT = "/opt/airflow/dbt"
DBT_BIN = "/opt/airflow/dbt-venv/bin/dbt"


@dag(
    schedule=[Asset("postgres://warehouse/raw/velib_station_status")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": alert_on_failure},
    tags=["velib", "final-project", "dbt"],
    doc_md=__doc__,
)
def velib_transform():
    BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_BIN} deps --project-dir {DBT} --profiles-dir {DBT} --quiet && "
                     f"{DBT_BIN} build --project-dir {DBT} --profiles-dir {DBT}",
        env={"PG_HOST": "postgres", "PG_PORT": "5432", "PG_DB": "warehouse", "PG_USER": "de", "PG_PASSWORD": "de",
             "DBT_TARGET_PATH": "/tmp/dbt-target", "DBT_LOG_PATH": "/tmp/dbt-logs"},
        append_env=True,
        outlets=[Asset("postgres://warehouse/velib_marts")],
    )


velib_transform()
