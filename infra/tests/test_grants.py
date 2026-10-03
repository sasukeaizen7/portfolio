"""Run after `terraform apply` (dev environment): checks that the roles can do exactly what they should."""

import os

import psycopg
import pytest

HOST = os.environ.get("PG_HOST", "localhost")
PORT = os.environ.get("PG_PORT", "5435")
pytestmark = pytest.mark.skipif(not os.environ.get("TF_VAR_analyst_password"), reason="run after terraform apply")


def connect(user: str, password: str):
    return psycopg.connect(f"postgresql://{user}:{password}@{HOST}:{PORT}/de", autocommit=True)


def can(conn, sql: str) -> bool:
    try:
        conn.execute(sql)
        return True
    except psycopg.errors.InsufficientPrivilege:
        return False


def test_analyst_reads_models_but_not_raw_and_cannot_write():
    with connect("dev_analyst", os.environ["TF_VAR_analyst_password"]) as c:
        assert can(c, "SELECT count(*) FROM dw.fact_sales")
        assert can(c, "SELECT count(*) FROM dbt_marts.fct_sales")
        assert not can(c, "SELECT count(*) FROM raw.sales_lines")
        assert not can(c, "DELETE FROM dw.fact_sales WHERE false")


def test_loader_writes_raw_only():
    with connect("dev_loader", os.environ["TF_VAR_loader_password"]) as c:
        assert can(c, "INSERT INTO raw.sales_lines SELECT * FROM raw.sales_lines WHERE false")
        assert not can(c, "SELECT count(*) FROM dw.fact_sales")


def test_default_privileges_cover_tables_created_later():
    with connect("de", os.environ.get("TF_VAR_pg_admin_password", "de")) as owner:
        owner.execute("DROP TABLE IF EXISTS dbt_marts.created_after_apply")
        owner.execute("CREATE TABLE dbt_marts.created_after_apply AS SELECT 1 AS x")
    try:
        with connect("dev_analyst", os.environ["TF_VAR_analyst_password"]) as c:
            assert can(c, "SELECT * FROM dbt_marts.created_after_apply")
    finally:
        with connect("de", os.environ.get("TF_VAR_pg_admin_password", "de")) as owner:
            owner.execute("DROP TABLE dbt_marts.created_after_apply")
