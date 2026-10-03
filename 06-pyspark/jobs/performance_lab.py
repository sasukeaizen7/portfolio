"""Performance experiments on 20M trips. Each one times two variants of the same query and saves the
physical plans, so the README can show WHY one is faster. Results: results/performance_lab.csv and
results/plans/*.txt. Timings depend on the machine; compare the ratios and the plans.
"""

from __future__ import annotations

import csv

from pyspark.sql import functions as F

from .common import CLEAN, RAW, RESULTS, get_spark, timed
from .transforms import add_salt, explode_salt

PLANS = RESULTS / "plans"


def plan(df, name: str) -> None:
    PLANS.mkdir(parents=True, exist_ok=True)
    (PLANS / f"{name}.txt").write_text(df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")), encoding="utf-8")


def run(label: str, df, rows: list, experiment: str) -> None:
    """Materialize df (a no-op write forces the full computation) and record the time."""
    timings: dict = {}
    with timed(label, timings):
        df.write.format("noop").mode("overwrite").save()
    rows.append({"experiment": experiment, "variant": label, "seconds": timings[label]})
    plan(df, f"{experiment}__{label.replace(' ', '_')}")


def main() -> None:
    spark = get_spark("performance_lab")
    rows: list[dict] = []
    trips = spark.read.parquet(str(CLEAN / "trips"))
    zones = (spark.read.option("header", True).csv(str(RAW / "taxi_zone_lookup.csv"))
             .select(F.col("LocationID").cast("int").alias("zone_id"), F.col("Borough").alias("borough")))

    # 1. Partition pruning: filter on the partition column (folders skipped) vs on an ordinary column.
    one_month = trips.where(F.col("pickup_month") == "2024-03").groupBy("payment").count()
    same_rows = trips.where(F.date_format("pickup_at", "yyyy-MM") == "2024-03").groupBy("payment").count()
    run("filter on partition column", one_month, rows, "partition_pruning")
    run("filter on derived column", same_rows, rows, "partition_pruning")

    # 2. Join strategy: broadcast the 265-row table vs force a sort-merge join (both sides shuffled).
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")     # stop AQE from switching to broadcast at runtime
    smj = trips.join(zones, trips.pickup_zone_id == zones.zone_id).groupBy("borough").agg(F.sum("total_amount"))
    run("sort-merge join", smj, rows, "join_strategy")
    bhj = trips.join(F.broadcast(zones), trips.pickup_zone_id == zones.zone_id).groupBy("borough").agg(F.sum("total_amount"))
    run("broadcast join", bhj, rows, "join_strategy")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

    # 3. Shuffle partitions: the default 200 for a small result vs AQE coalescing them at runtime.
    agg = trips.groupBy("pickup_zone_id", "pickup_hour").agg(F.avg("total_amount"))
    run("200 shuffle partitions, AQE off", agg, rows, "shuffle_partitions")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    run("AQE coalesces partitions", agg, rows, "shuffle_partitions")

    # 4. Skew: one zone (JFK-area pickups) dominates a self-join-like lookup. Salting spreads the hot key.
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    hot = trips.where(F.col("pickup_zone_id").isin(132, 138, 161, 236, 237))     # a few very busy zones
    lookup = hot.groupBy("pickup_zone_id").agg(F.avg("total_amount").alias("zone_avg"))
    plain = hot.join(lookup, "pickup_zone_id").groupBy("pickup_zone_id").agg(F.max("zone_avg"))
    run("skewed key, no mitigation", plain, rows, "skew")
    buckets = 16
    salted = (add_salt(hot, buckets).join(explode_salt(lookup, buckets), ["pickup_zone_id", "salt"])
              .groupBy("pickup_zone_id").agg(F.max("zone_avg")))
    run("salted key (16 buckets)", salted, rows, "skew")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

    # 5. Caching: the same DataFrame used by 3 actions, recomputed each time vs cached once.
    base = trips.where(F.col("distance_mi") > 5).select("pickup_zone_id", "payment", "total_amount", "pickup_hour")
    t: dict = {}
    with timed("3 actions without cache", t):
        for col in ("pickup_zone_id", "payment", "pickup_hour"):
            base.groupBy(col).count().collect()
    rows.append({"experiment": "caching", "variant": "3 actions without cache", "seconds": t["3 actions without cache"]})
    cached = base.cache()
    cached.count()                                             # materialize the cache (not timed)
    with timed("3 actions on cached data", t):
        for col in ("pickup_zone_id", "payment", "pickup_hour"):
            cached.groupBy(col).count().collect()
    rows.append({"experiment": "caching", "variant": "3 actions on cached data", "seconds": t["3 actions on cached data"]})
    cached.unpersist()

    RESULTS.mkdir(parents=True, exist_ok=True)
    with open(RESULTS / "performance_lab.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["experiment", "variant", "seconds"])
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
