"""Spark job: landing CSV -> clean Parquet partitioned by contact month. Submitted by Airflow.

    spark-submit jobs/clean_campaign.py --input data/landing/bank-additional-full.csv --output data/clean/contacts
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, __file__.rsplit("/", 1)[0])          # make campaign_transforms importable under spark-submit
from campaign_transforms import add_features, derive_contact_month, quality_issues, snake_case_columns  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()

    spark = SparkSession.builder.appName("clean_campaign").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.options(header=True, sep=";", inferSchema=True).csv(args.input)
    # Keep the file order: read as ONE partition and number the rows (monotonically_increasing_id is
    # ordered within a partition). The year derivation depends on that order.
    raw = raw.coalesce(1).withColumn("row_number", F.monotonically_increasing_id())
    df = add_features(derive_contact_month(snake_case_columns(raw)))

    issues = quality_issues(df)
    print(json.dumps(issues, indent=2))
    if any(issues.values()):
        raise SystemExit(f"quality checks failed: { {k: v for k, v in issues.items() if v} }")

    (df.drop("y", "month_num")
       .repartition("partition_month")
       .write.mode("overwrite").partitionBy("partition_month").parquet(args.output))
    print(f"wrote {df.count():,} contacts to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
