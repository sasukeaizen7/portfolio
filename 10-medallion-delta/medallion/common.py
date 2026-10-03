"""Spark + Delta session, table paths, and the small pipeline-state table."""

from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DATA = Path(os.environ.get("MEDALLION_DATA", "/app/data"))
LANDING = DATA / "landing" / "clicks"
DELTA = DATA / "delta"
BRONZE = str(DELTA / "bronze" / "clicks_raw")
INGESTED_FILES = str(DELTA / "bronze" / "_ingested_files")
SILVER = str(DELTA / "silver" / "clicks")
QUARANTINE = str(DELTA / "silver" / "clicks_quarantine")
GOLD_FUNNEL = str(DELTA / "gold" / "daily_funnel")
GOLD_PRODUCTS = str(DELTA / "gold" / "product_daily")
STATE = str(DELTA / "_pipeline_state")


def get_spark(app: str = "medallion") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        # Every new Delta table records its row-level changes (Change Data Feed): silver and gold read
        # only what changed upstream since their last run.
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_state(spark: SparkSession, key: str, default: int = -1) -> int:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, STATE):
        return default
    rows = spark.read.format("delta").load(STATE).where(f"key = '{key}'").collect()
    return rows[0]["value"] if rows else default


def set_state(spark: SparkSession, key: str, value: int) -> None:
    from delta.tables import DeltaTable

    df = spark.createDataFrame([(key, value)], "key string, value long")
    if not DeltaTable.isDeltaTable(spark, STATE):
        df.write.format("delta").save(STATE)
        return
    (DeltaTable.forPath(spark, STATE).alias("s").merge(df.alias("n"), "s.key = n.key")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
