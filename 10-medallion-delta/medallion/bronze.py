"""BRONZE: raw data, as received, append-only. Every line is kept as a string (even malformed ones),
with lineage columns. New files only: processed files are recorded in a Delta table, which is what
Databricks Auto Loader does for you (with its checkpoint)."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .common import BRONZE, INGESTED_FILES, LANDING


def ingest_new_files(spark: SparkSession) -> int:
    all_files = sorted(p.as_posix() for p in LANDING.glob("date=*/*.json"))
    seen = set()
    if DeltaTable.isDeltaTable(spark, INGESTED_FILES):
        seen = {r["path"] for r in spark.read.format("delta").load(INGESTED_FILES).collect()}
    new = [f for f in all_files if f not in seen]
    if not new:
        return 0
    raw = (spark.read.text(new)
           .withColumn("_source_file", F.input_file_name())
           .withColumn("_ingested_at", F.current_timestamp())
           .withColumn("_landing_date", F.regexp_extract("_source_file", r"date=(\d{4}-\d{2}-\d{2})", 1).cast("date"))
           .where(F.length("value") > 0))
    n = raw.count()
    raw.write.format("delta").mode("append").partitionBy("_landing_date").save(BRONZE)
    (spark.createDataFrame([(f,) for f in new], "path string").withColumn("ingested_at", F.current_timestamp())
     .write.format("delta").mode("append").save(INGESTED_FILES))
    return n
