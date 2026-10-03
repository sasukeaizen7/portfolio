"""Raw monthly Parquet files -> one clean dataset partitioned by pickup month, plus a quality report."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import CLEAN, RAW, RESULTS, get_spark, timed
from .download import MONTHS
from .transforms import quality_verdict, to_clean_trips, unify


def main() -> None:
    spark = get_spark("clean_trips")
    # Each month is unified to the same schema before the union (the raw files disagree on types/case).
    months = [unify(spark.read.parquet(str(RAW / "yellow" / f"yellow_tripdata_{m}.parquet"))) for m in MONTHS]
    raw = reduce(lambda a, b: a.unionByName(b), months)

    year, last = MONTHS[-1].split("-")
    end = f"{year}-{int(last) + 1:02d}-01"
    verdicts = quality_verdict(raw, f"{MONTHS[0]}-01", end).cache()   # read twice: report + clean write

    with timed("quality report"):
        report = (verdicts.groupBy("quality").count()
                  .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy()), 2))
                  .orderBy(F.desc("count")))
        RESULTS.mkdir(parents=True, exist_ok=True)
        report.toPandas().to_csv(RESULTS / "quality_report.csv", index=False)
        report.show(truncate=False)

    with timed("write clean trips, partitioned by month"):
        (to_clean_trips(verdicts)
         .repartition("pickup_month")              # one task per month -> one large file per partition
         .write.mode("overwrite")
         .partitionBy("pickup_month")
         .parquet(str(CLEAN / "trips")))
    verdicts.unpersist()
    print(f"  clean trips: {spark.read.parquet(str(CLEAN / 'trips')).count():,}")


if __name__ == "__main__":
    main()
