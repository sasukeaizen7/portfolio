"""Delta table maintenance and time travel on the silver table."""

from __future__ import annotations

from delta.tables import DeltaTable

from .common import SILVER, get_spark


def main() -> None:
    spark = get_spark("time_travel")
    table = DeltaTable.forPath(spark, SILVER)
    history = table.history().select("version", "timestamp", "operation", "operationMetrics.numOutputRows")
    history.orderBy("version").show(truncate=False)

    latest = table.history(1).collect()[0]["version"]
    v1 = spark.read.format("delta").option("versionAsOf", 1).load(SILVER).count()
    now = spark.read.format("delta").load(SILVER).count()
    print(f"time travel: {v1:,} rows at version 1, {now:,} rows at version {latest}")

    # Compact small files and co-locate rows by user (faster per-user queries), then clean old files.
    print("OPTIMIZE ZORDER BY (user_id):", table.optimize().executeZOrderBy("user_id").select("metrics.numFilesAdded",
                                                                                               "metrics.numFilesRemoved").collect())
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")   # demo only: no 7-day wait
    table.vacuum(0)
    print("VACUUM done: files no longer referenced are deleted (time travel to old versions is now limited)")


if __name__ == "__main__":
    main()
