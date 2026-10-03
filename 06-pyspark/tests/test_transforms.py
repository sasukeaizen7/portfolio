from datetime import datetime

from pyspark.sql import Row

from jobs.transforms import (explode_salt, add_salt, quality_verdict, revenue_by_zone_month, tip_rate_by_distance,
                             to_clean_trips, top_n_per_group, unify)


def raw_trip(pickup="2024-01-10 08:00", dropoff="2024-01-10 08:20", distance=2.0, fare=12.0, total=15.0,
             payment=1, zone=132, **extra):
    row = dict(tpep_pickup_datetime=datetime.fromisoformat(pickup), tpep_dropoff_datetime=datetime.fromisoformat(dropoff),
               passenger_count=1, trip_distance=distance, PULocationID=zone, DOLocationID=1, payment_type=payment,
               fare_amount=fare, tip_amount=2.0, tolls_amount=0.0, total_amount=total)
    row.update(extra)
    return row


def test_unify_handles_column_case_and_types(spark):
    df = spark.createDataFrame([Row(**{**raw_trip(), "Airport_fee": 1.75})]).withColumnRenamed("passenger_count", "Passenger_Count")
    out = unify(df)
    assert out.columns[:3] == ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count"]
    assert dict(out.dtypes)["passenger_count"] == "int"


def test_quality_rules_first_failure_wins(spark):
    rows = [raw_trip(),                                                     # ok
            raw_trip(pickup="2023-12-31 23:00", dropoff="2023-12-31 23:10"),  # outside period
            raw_trip(dropoff="2024-01-10 07:00"),                           # dropoff before pickup
            raw_trip(distance=0.0, fare=-1.0),                              # distance AND fare bad -> distance wins
            raw_trip(total=0.0)]                                            # non-positive fare
    verdicts = [r.quality for r in quality_verdict(spark.createDataFrame(rows), "2024-01-01", "2024-07-01").collect()]
    assert verdicts == ["ok", "pickup outside period", "dropoff not after pickup", "distance not in (0, 100] miles",
                        "non-positive fare"]


def test_clean_trips_keeps_valid_rows_and_adds_features(spark):
    df = quality_verdict(spark.createDataFrame([raw_trip(pickup="2024-01-13 09:15", dropoff="2024-01-13 09:30"),
                                                raw_trip(distance=0.0)]), "2024-01-01", "2024-07-01")
    out = to_clean_trips(df).collect()
    assert len(out) == 1
    t = out[0]
    assert (t.pickup_month, t.pickup_hour, t.is_weekend, t.payment, t.duration_min) == ("2024-01", 9, True, "Credit card", 15.0)


def test_top_n_per_group_is_deterministic_on_ties(spark):
    df = spark.createDataFrame([("A", "x", 10), ("A", "y", 10), ("A", "z", 5), ("B", "w", 1)], ["g", "item", "v"])
    out = sorted((r.g, r.item, r.rank) for r in top_n_per_group(df, ["g"], "v", 2).collect())
    assert out == [("A", "x", 1), ("A", "y", 2), ("B", "w", 1)]


def test_revenue_by_zone_month_joins_and_aggregates(spark):
    trips = to_clean_trips(quality_verdict(spark.createDataFrame([raw_trip(total=10.0), raw_trip(total=20.0),
                                                                    raw_trip(zone=1, total=5.0)]),
                                           "2024-01-01", "2024-07-01"))
    zones = spark.createDataFrame([(132, "Queens", "JFK Airport"), (1, "EWR", "Newark Airport")], ["zone_id", "borough", "zone"])
    out = {r.zone: (r.trips, r.revenue_usd) for r in revenue_by_zone_month(trips, zones).collect()}
    assert out == {"JFK Airport": (2, 30.0), "Newark Airport": (1, 5.0)}


def test_tip_rate_only_counts_card_payments(spark):
    trips = to_clean_trips(quality_verdict(spark.createDataFrame([raw_trip(distance=0.5, fare=10.0),
                                                                    raw_trip(distance=0.5, payment=2)]),
                                           "2024-01-01", "2024-07-01"))
    (row,) = tip_rate_by_distance(trips).collect()
    assert (row.distance_band, row.trips, row.avg_tip_pct) == ("1: under 1 mi", 1, 20.0)


def test_salting_preserves_join_results(spark):
    big = spark.createDataFrame([(1, v) for v in range(100)] + [(2, 5)], ["k", "v"])
    small = spark.createDataFrame([(1, "one"), (2, "two")], ["k", "name"])
    salted = add_salt(big, 8).join(explode_salt(small, 8), ["k", "salt"])
    assert salted.count() == big.join(small, "k").count() == 101
