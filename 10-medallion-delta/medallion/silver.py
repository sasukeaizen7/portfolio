"""SILVER: parsed, typed, validated, de-duplicated. Reads only the bronze rows added since the last run
(Delta Change Data Feed), then:
  * valid events  -> MERGE into silver on event_id (insert new, ignore duplicates): idempotent;
  * invalid lines -> the quarantine table, with the reason, never silently dropped.
New source columns (like `device` from day 3) are added to silver automatically (schema evolution);
wrong types are still refused (schema enforcement)."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from .common import BRONZE, QUARANTINE, SILVER, get_state, set_state

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("event_time", StringType()),
    StructField("user_id", StringType()), StructField("session_id", StringType()),
    StructField("event_type", StringType()), StructField("page", StringType()),
    StructField("product_id", StringType()), StructField("price", DoubleType()),
])
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase"]


def batch_schema(bronze_rows: DataFrame) -> StructType:
    """The known schema, plus any top-level field the source started sending (kept as string).
    This is how the `device` field appearing on day 3 reaches silver without a code change."""
    keys = {r[0] for r in bronze_rows.select(F.explode(F.json_object_keys("value"))).distinct().collect()}
    extra = sorted(keys - set(EVENT_SCHEMA.fieldNames()))
    return StructType(EVENT_SCHEMA.fields + [StructField(k, StringType()) for k in extra])


def parse_and_validate(bronze_rows: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed = bronze_rows.withColumn("e", F.from_json("value", batch_schema(bronze_rows), {"mode": "PERMISSIVE"}))
    reason = (F.when(F.col("e").isNull() | F.col("e.event_id").isNull(), "unparseable or no event_id")
              .when(~F.col("e.event_type").isin(EVENT_TYPES), "unknown event_type")
              .when(F.to_timestamp("e.event_time").isNull(), "bad event_time")
              .when(F.col("e.event_type").isin("add_to_cart", "purchase") & F.col("e.product_id").isNull(),
                    "cart/purchase without product"))
    tagged = parsed.withColumn("reject_reason", reason)
    valid = (tagged.where(F.col("reject_reason").isNull())
             .select("e.*", "_source_file", "_landing_date")
             .withColumn("event_time", F.to_timestamp("event_time"))
             .withColumn("event_date", F.to_date("event_time"))
             # Keep one row per event_id even inside a single batch (MERGE needs unique source keys).
             .dropDuplicates(["event_id"]))
    invalid = tagged.where(F.col("reject_reason").isNotNull()).select("value", "reject_reason", "_source_file", "_landing_date")
    return valid, invalid


def process(spark: SparkSession) -> dict:
    last = get_state(spark, "silver.bronze_version")
    current = DeltaTable.forPath(spark, BRONZE).history(1).collect()[0]["version"]
    if current <= last:
        return {"new_bronze_rows": 0}
    changes = (spark.read.format("delta").option("readChangeFeed", "true")
               .option("startingVersion", last + 1).option("endingVersion", current).load(BRONZE)
               .where(F.col("_change_type") == "insert"))
    valid, invalid = parse_and_validate(changes)

    if not DeltaTable.isDeltaTable(spark, SILVER):
        valid.limit(0).write.format("delta").partitionBy("event_date").save(SILVER)
    before = spark.read.format("delta").load(SILVER).count()
    (DeltaTable.forPath(spark, SILVER).alias("s")
     .merge(valid.alias("n"), "s.event_id = n.event_id")
     .whenNotMatchedInsertAll()
     .withSchemaEvolution()                                 # new source columns become silver columns
     .execute())
    after = spark.read.format("delta").load(SILVER).count()
    invalid.write.format("delta").mode("append").option("mergeSchema", "true").save(QUARANTINE)
    set_state(spark, "silver.bronze_version", current)
    return {"new_bronze_rows": changes.count(), "inserted": after - before, "quarantined": invalid.count()}
