"""GOLD: business tables. Only the dates touched by new silver rows are recomputed (found through
silver's Change Data Feed), and written with replaceWhere: an atomic overwrite of just those dates.
Late events therefore update the day they belong to, and re-running changes nothing."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .common import GOLD_FUNNEL, GOLD_PRODUCTS, SILVER, get_state, set_state


def daily_funnel(silver_days):
    """Per day: sessions, and the share that reached product view, cart and purchase."""
    sessions = (silver_days.groupBy("event_date", "session_id")
                .agg(F.max((F.col("event_type") == "product_view").cast("int")).alias("viewed"),
                     F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
                     F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased")))
    return (sessions.groupBy("event_date")
            .agg(F.count("*").alias("sessions"),
                 F.round(100 * F.avg("viewed"), 2).alias("product_view_pct"),
                 F.round(100 * F.avg("carted"), 2).alias("cart_pct"),
                 F.round(100 * F.avg("purchased"), 2).alias("purchase_pct")))


def product_daily(silver_days):
    return (silver_days.where(F.col("product_id").isNotNull())
            .groupBy("event_date", "product_id")
            .agg(F.sum((F.col("event_type") == "product_view").cast("int")).alias("views"),
                 F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
                 F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))), 2).alias("revenue")))


def process(spark: SparkSession) -> dict:
    last = get_state(spark, "gold.silver_version")
    current = DeltaTable.forPath(spark, SILVER).history(1).collect()[0]["version"]
    if current <= last:
        return {"dates_recomputed": []}
    changed = (spark.read.format("delta").option("readChangeFeed", "true")
               .option("startingVersion", last + 1).option("endingVersion", current).load(SILVER)
               .select("event_date").distinct())
    dates = sorted(str(r["event_date"]) for r in changed.collect())
    silver_days = spark.read.format("delta").load(SILVER).where(F.col("event_date").isin(dates))
    condition = "event_date IN (" + ", ".join(f"DATE'{d}'" for d in dates) + ")"
    for path, df in ((GOLD_FUNNEL, daily_funnel(silver_days)), (GOLD_PRODUCTS, product_daily(silver_days))):
        (df.write.format("delta").mode("overwrite").option("replaceWhere", condition)
         .partitionBy("event_date").save(path))
    set_state(spark, "gold.silver_version", current)
    return {"dates_recomputed": dates}
