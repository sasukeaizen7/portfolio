"""End-to-end medallion tests on small landed days (run in the container: needs Spark + Delta)."""

import importlib

import pytest

pytest.importorskip("delta")


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    import os

    os.environ["MEDALLION_DATA"] = str(tmp_path_factory.mktemp("medallion"))
    from medallion import bronze, common, gold, land, silver
    for m in (common, land, bronze, silver, gold):          # re-read MEDALLION_DATA
        importlib.reload(m)
    spark = common.get_spark("tests")
    return spark, common, land, bronze, silver, gold


def test_full_flow_is_incremental_idempotent_and_evolves_schema(pipeline):
    spark, common, land, bronze, silver, gold = pipeline
    from pyspark.sql import functions as F

    land.land_day("2026-09-28", events=2400, seed=1)
    assert bronze.ingest_new_files(spark) > 2000
    s1 = silver.process(spark)
    assert s1["inserted"] > 0 and s1["quarantined"] > 0              # malformed lines are kept aside
    g1 = gold.process(spark)
    assert "2026-09-28" in g1["dates_recomputed"]

    # Nothing new: every layer is a no-op.
    assert bronze.ingest_new_files(spark) == 0
    assert silver.process(spark) == {"new_bronze_rows": 0}
    assert gold.process(spark) == {"dates_recomputed": []}

    # Day 2 re-sends day 1's file content (a duplicate delivery) and adds the `device` field.
    land.land_day("2026-09-29", events=2400, seed=1, add_device=True)
    bronze.ingest_new_files(spark)
    s2 = silver.process(spark)
    silver_df = spark.read.format("delta").load(common.SILVER)
    assert "device" in silver_df.columns                              # schema evolved
    assert s2["inserted"] == 0                                        # same event ids: MERGE inserted nothing
    assert silver_df.select("event_id").distinct().count() == silver_df.count()
    assert silver_df.where(F.col("device").isNotNull()).count() == 0  # old rows weren't rewritten


def test_quarantine_explains_rejections(pipeline):
    spark, common, *_ = pipeline
    reasons = {r["reject_reason"] for r in spark.read.format("delta").load(common.QUARANTINE).collect()}
    assert reasons and reasons <= {"unparseable or no event_id", "unknown event_type", "bad event_time",
                                   "cart/purchase without product"}


def test_schema_enforcement_refuses_wrong_types(pipeline):
    spark, common, *_ = pipeline
    bad = spark.createDataFrame([("x", 123)], "event_id string, event_time int")
    with pytest.raises(Exception, match="(?i)schema|type|merge"):
        bad.write.format("delta").mode("append").save(common.SILVER)


def test_gold_funnel_is_consistent(pipeline):
    spark, common, *_ = pipeline
    rows = spark.read.format("delta").load(common.GOLD_FUNNEL).collect()
    assert rows
    for row in rows:
        assert row["sessions"] > 0
        for pct in ("product_view_pct", "cart_pct", "purchase_pct"):
            assert 0 <= row[pct] <= 100
        assert row["purchase_pct"] < row["product_view_pct"]       # few sessions buy, many browse
