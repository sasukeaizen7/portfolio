"""Land 3 days of clicks (day 3 with a new `device` field) and run bronze -> silver -> gold after each."""

from __future__ import annotations

from . import bronze, gold, silver
from .common import GOLD_FUNNEL, SILVER, get_spark
from .land import land_day

DAYS = [("2026-09-28", False), ("2026-09-29", False), ("2026-09-30", True)]


def main() -> None:
    spark = get_spark()
    for day, with_device in DAYS:
        landed = land_day(day, add_device=with_device)
        print(f"== {day}: landed {landed:,} lines" + (" (source now sends `device`)" if with_device else ""))
        print(f"   bronze: +{bronze.ingest_new_files(spark):,} raw rows")
        print(f"   silver: {silver.process(spark)}")
        print(f"   gold:   {gold.process(spark)}")
    print("== re-run without new files (must be a no-op)")
    print(f"   bronze +{bronze.ingest_new_files(spark)}, silver {silver.process(spark)}, gold {gold.process(spark)}")
    spark.read.format("delta").load(GOLD_FUNNEL).orderBy("event_date").show()
    spark.read.format("delta").load(SILVER).groupBy("event_date", "device").count().orderBy("event_date", "device").show()


if __name__ == "__main__":
    main()
