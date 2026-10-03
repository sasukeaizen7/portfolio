"""Pure DataFrame -> DataFrame transformations. No I/O, so each one is unit-tested on tiny inputs."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# The monthly files don't share one schema (e.g. `Airport_fee` vs `airport_fee`, int vs bigint
# passenger_count). Selecting and casting explicitly gives every month the same schema.
UNIFIED_COLUMNS = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "total_amount": "double",
}
PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute"}


def unify(df: DataFrame) -> DataFrame:
    lower = {c.lower(): c for c in df.columns}
    return df.select([F.col(lower[name.lower()]).cast(dtype).alias(name) for name, dtype in UNIFIED_COLUMNS.items()])


def quality_verdict(df: DataFrame, first_month: str, end_month_exclusive: str) -> DataFrame:
    """One verdict per raw trip (the first failing rule wins), so every rejected row is explained."""
    duration_min = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    verdict = (
        F.when((F.col("tpep_pickup_datetime") < F.lit(first_month).cast("timestamp"))
               | (F.col("tpep_pickup_datetime") >= F.lit(end_month_exclusive).cast("timestamp")), "pickup outside period")
        .when(duration_min <= 0, "dropoff not after pickup")
        .when(duration_min > 180, "longer than 3 hours")
        .when((F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100), "distance not in (0, 100] miles")
        .when((F.col("fare_amount") <= 0) | (F.col("total_amount") <= 0), "non-positive fare")
        .otherwise("ok")
    )
    return df.withColumn("duration_min", duration_min).withColumn("quality", verdict)


def to_clean_trips(df: DataFrame) -> DataFrame:
    """Keep valid trips, rename to snake_case, add the partition column and a few features."""
    payment = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    return (
        df.where(F.col("quality") == "ok")
        .select(
            F.col("tpep_pickup_datetime").alias("pickup_at"),
            F.col("tpep_dropoff_datetime").alias("dropoff_at"),
            F.round("duration_min", 2).alias("duration_min"),
            "passenger_count",
            F.col("trip_distance").alias("distance_mi"),
            F.col("PULocationID").alias("pickup_zone_id"),
            F.col("DOLocationID").alias("dropoff_zone_id"),
            F.coalesce(payment[F.col("payment_type")], F.lit("Other")).alias("payment"),
            "fare_amount", "tip_amount", "tolls_amount", "total_amount",
        )
        .withColumn("pickup_month", F.date_format("pickup_at", "yyyy-MM"))
        .withColumn("pickup_hour", F.hour("pickup_at"))
        .withColumn("is_weekend", F.dayofweek("pickup_at").isin(1, 7))
    )


def top_n_per_group(df: DataFrame, group_cols: list[str], order_col: str, n: int) -> DataFrame:
    """Top n rows per group by order_col (ties broken deterministically by every other column)."""
    tiebreak = [F.col(c) for c in df.columns if c not in group_cols + [order_col]]
    w = Window.partitionBy(*group_cols).orderBy(F.col(order_col).desc(), *tiebreak)
    return df.withColumn("rank", F.row_number().over(w)).where(F.col("rank") <= n)


def revenue_by_zone_month(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Monthly trips and revenue per pickup zone. `zones` (265 rows) is broadcast to every executor,
    so the 20M-row side is never shuffled for the join."""
    return (
        trips.join(F.broadcast(zones), trips.pickup_zone_id == zones.zone_id)
        .groupBy("pickup_month", "borough", "zone")
        .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue_usd"))
    )


def tip_rate_by_distance(trips: DataFrame) -> DataFrame:
    band = (F.when(F.col("distance_mi") < 1, "1: under 1 mi")
            .when(F.col("distance_mi") < 3, "2: 1-3 mi")
            .when(F.col("distance_mi") < 10, "3: 3-10 mi")
            .otherwise("4: 10+ mi"))
    return (
        trips.where(F.col("payment") == "Credit card")
        .withColumn("distance_band", band)
        .groupBy("distance_band")
        .agg(F.count("*").alias("trips"),
             F.round(100 * F.avg(F.col("tip_amount") / F.col("fare_amount")), 1).alias("avg_tip_pct"))
        .orderBy("distance_band")
    )


def add_salt(df: DataFrame, salt_buckets: int) -> DataFrame:
    """Skew fix: give each row of the big side a random salt in [0, salt_buckets); join on (key, salt)
    against the small side exploded to every salt value, so one hot key is spread over many tasks."""
    return df.withColumn("salt", (F.rand(seed=42) * salt_buckets).cast("int"))


def explode_salt(df: DataFrame, salt_buckets: int) -> DataFrame:
    return df.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(salt_buckets)])))
