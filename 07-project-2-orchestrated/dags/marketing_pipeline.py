"""Project 2: download -> Spark (clean, partition) -> Postgres -> dbt (models + tests) -> Asset.

Each step uses the right tool: Python for I/O, Spark for the transformation, dbt for SQL modeling
and testing, Airflow to order them, retry them and show what failed.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task

AIRFLOW_HOME = Path("/opt/airflow")
LANDING = AIRFLOW_HOME / "data" / "landing" / "marketing"
CLEAN = AIRFLOW_HOME / "data" / "clean" / "marketing_contacts"
DBT = AIRFLOW_HOME / "dbt"
MARTS = Asset("postgres://warehouse/marketing_marts")


@dag(
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["marketing", "spark", "dbt", "portfolio"],
    doc_md=__doc__,
)
def marketing_pipeline():
    @task
    def land() -> str:
        from marketing import land as land_file

        return str(land_file(LANDING))

    clean = SparkSubmitOperator(
        task_id="spark_clean",
        conn_id="spark_local",                    # local[*]: Spark runs inside the task's container
        application=str(AIRFLOW_HOME / "jobs" / "clean_campaign.py"),
        py_files=str(AIRFLOW_HOME / "jobs" / "campaign_transforms.py"),
        application_args=["--input", "{{ ti.xcom_pull(task_ids='land') }}", "--output", str(CLEAN)],
        conf={"spark.driver.memory": "2g", "spark.sql.shuffle.partitions": "8"},
    )

    @task
    def load_warehouse() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from marketing import load_parquet_to_postgres

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        try:
            return load_parquet_to_postgres(CLEAN, conn)
        finally:
            conn.close()

    dbt_env = {"PG_HOST": "postgres", "PG_PORT": "5432", "PG_DB": "warehouse", "PG_USER": "de", "PG_PASSWORD": "de",
               "DBT_TARGET_PATH": "/tmp/dbt-target", "DBT_LOG_PATH": "/tmp/dbt-logs"}
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"/opt/airflow/dbt-venv/bin/dbt deps --project-dir {DBT} --profiles-dir {DBT} && "
                     f"/opt/airflow/dbt-venv/bin/dbt build --project-dir {DBT} --profiles-dir {DBT}",
        env=dbt_env,
        append_env=True,
        outlets=[MARTS],
    )

    land() >> clean >> load_warehouse() >> dbt_build


marketing_pipeline()
