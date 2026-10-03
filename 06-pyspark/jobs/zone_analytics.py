"""Analytics on the clean trips with the DataFrame API and Spark SQL (same questions, two APIs)."""

from __future__ import annotations

from pyspark.sql import functions as F

from .common import CLEAN, RAW, RESULTS, get_spark, timed
from .transforms import revenue_by_zone_month, tip_rate_by_distance, top_n_per_group


def save(df, name: str) -> None:
    """Results are small: write them as one CSV each (committed, readable on GitHub)."""
    df.toPandas().to_csv(RESULTS / f"{name}.csv", index=False)


def main() -> None:
    spark = get_spark("zone_analytics")
    trips = spark.read.parquet(str(CLEAN / "trips"))
    zones = (spark.read.option("header", True).csv(str(RAW / "taxi_zone_lookup.csv"))
             .select(F.col("LocationID").cast("int").alias("zone_id"), F.col("Borough").alias("borough"),
                     F.col("Zone").alias("zone")))
    RESULTS.mkdir(parents=True, exist_ok=True)

    with timed("top 3 zones per borough per month (broadcast join + window)"):
        monthly = revenue_by_zone_month(trips, zones)
        top = top_n_per_group(monthly.where(~F.col("borough").isin("Unknown", "N/A")),
                              ["pickup_month", "borough"], "revenue_usd", 3)
        save(top.orderBy("pickup_month", "borough", "rank"), "top_zones_per_borough_month")

    with timed("tip rate by distance band"):
        save(tip_rate_by_distance(trips), "tip_rate_by_distance")

    # The same kind of question in Spark SQL: register views, write SQL. Both APIs compile to the
    # same optimized plan (compare with .explain()).
    trips.createOrReplaceTempView("trips")
    zones.createOrReplaceTempView("zones")
    with timed("Spark SQL: monthly KPIs with month-over-month growth"):
        kpis = spark.sql("""
            WITH monthly AS (
                SELECT pickup_month,
                       count(*)                                   AS trips,
                       round(sum(total_amount), 0)                AS revenue_usd,
                       round(avg(distance_mi), 2)                 AS avg_distance_mi,
                       round(avg(total_amount), 2)                AS avg_ticket_usd,
                       round(100 * avg(CASE WHEN is_weekend THEN 1 ELSE 0 END), 1) AS weekend_pct
                FROM trips
                GROUP BY pickup_month
            )
            SELECT *,
                   round(100 * (revenue_usd / lag(revenue_usd) OVER (ORDER BY pickup_month) - 1), 1) AS revenue_mom_pct
            FROM monthly
            ORDER BY pickup_month
        """)
        save(kpis, "monthly_kpis")
        kpis.show()

    with timed("Spark SQL: busiest hour per weekday type"):
        save(spark.sql("""
            SELECT is_weekend, pickup_hour, count(*) / count(DISTINCT to_date(pickup_at)) AS avg_trips_per_day
            FROM trips GROUP BY is_weekend, pickup_hour
            ORDER BY is_weekend, pickup_hour
        """), "hourly_profile")


if __name__ == "__main__":
    main()
