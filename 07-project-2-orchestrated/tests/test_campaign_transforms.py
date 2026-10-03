"""Spark transformation tests (need Java 17 + pyspark: run inside the Airflow image or the 06 Spark image)."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from campaign_transforms import add_features, derive_contact_month, quality_issues, snake_case_columns  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    yield s
    s.stop()


def test_year_is_derived_from_month_wraps(spark):
    months = ["may", "may", "nov", "dec", "mar", "dec", "apr", "nov"]
    df = spark.createDataFrame([(i, m) for i, m in enumerate(months)], ["row_number", "month"])
    out = [str(r.contact_month) for r in derive_contact_month(df).orderBy("row_number").collect()]
    assert out == ["2008-05-01", "2008-05-01", "2008-11-01", "2008-12-01", "2009-03-01", "2009-12-01",
                   "2010-04-01", "2010-11-01"]


def test_features_and_sentinel_values(spark):
    df = spark.createDataFrame([(0, "may", 30, 999, "yes"), (1, "may", 70, 6, "no")],
                               ["row_number", "month", "age", "pdays", "y"])
    rows = add_features(derive_contact_month(df)).orderBy("row_number").collect()
    assert (rows[0].converted, rows[0].age_band, rows[0].previously_contacted, rows[0].days_since_previous) == (True, "25-34", False, None)
    assert (rows[1].age_band, rows[1].previously_contacted, rows[1].days_since_previous) == ("65+", True, 6)


def test_snake_case_and_quality_rules(spark):
    df = spark.createDataFrame([(0, "may", 40, 1, 999, "no", 1.1), (1, "xyz", 15, 0, 999, "maybe", 1.1)],
                               ["row_number", "month", "age", "campaign", "pdays", "y", "emp.var.rate"])
    df = add_features(derive_contact_month(snake_case_columns(df)))
    assert "emp_var_rate" in df.columns
    issues = quality_issues(df)
    assert issues["month name not recognized"] == 1 and issues["age outside 17-100"] == 1
    assert issues["campaign contacts < 1"] == 1 and issues["outcome not yes/no"] == 1
