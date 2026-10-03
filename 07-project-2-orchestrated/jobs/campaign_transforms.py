"""Pure transformations of the bank-marketing calls (unit-tested on tiny DataFrames)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
FIRST_YEAR = 2008      # the dataset documentation: calls from May 2008 to November 2010, in date order


def snake_case_columns(df: DataFrame) -> DataFrame:
    """emp.var.rate -> emp_var_rate (dots in column names break SQL and Spark column references)."""
    return df.toDF(*[c.replace(".", "_").lower() for c in df.columns])


def derive_contact_month(df: DataFrame, order_col: str = "row_number") -> DataFrame:
    """The file has a month name but no year. Rows are in date order, so every time the month number goes
    DOWN (dec -> mar), a new year has started: year = first year + running count of those wraps."""
    position = F.array_position(F.array(*[F.lit(m) for m in MONTHS]), F.lower("month"))   # 0 if not found
    month_num = F.when(position > 0, position).cast("int")
    w = Window.orderBy(order_col)          # a single global order: fine for 41k rows, not for billions
    wrapped = F.when(month_num < F.lag(month_num).over(w), 1).otherwise(0)
    return (
        df.withColumn("month_num", month_num)
        .withColumn("wrapped", wrapped)
        .withColumn("year", F.lit(FIRST_YEAR) + F.sum("wrapped").over(w.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("contact_month", F.make_date("year", "month_num", F.lit(1)))
        .drop("wrapped")
    )


def add_features(df: DataFrame) -> DataFrame:
    age_band = (F.when(F.col("age") < 25, "18-24").when(F.col("age") < 35, "25-34").when(F.col("age") < 45, "35-44")
                .when(F.col("age") < 55, "45-54").when(F.col("age") < 65, "55-64").otherwise("65+"))
    return (
        df.withColumn("converted", F.col("y") == "yes")
        .withColumn("age_band", age_band)
        # 999 means "never contacted in a previous campaign": a sentinel value, not 999 days.
        .withColumn("previously_contacted", F.col("pdays") != 999)
        .withColumn("days_since_previous", F.when(F.col("pdays") != 999, F.col("pdays")))
        .withColumn("partition_month", F.date_format("contact_month", "yyyy-MM"))
    )


def quality_issues(df: DataFrame) -> dict[str, int]:
    """Counts of rows breaking each rule (all must be 0 for the job to write its output)."""
    return {
        "month name not recognized": df.where(F.col("month_num").isNull()).count(),
        "age outside 17-100": df.where(~F.col("age").between(17, 100)).count(),
        "campaign contacts < 1": df.where(F.col("campaign") < 1).count(),
        "outcome not yes/no": df.where(~F.col("y").isin("yes", "no")).count(),
        "contact month outside May 2008 - Nov 2010": df.where(
            ~F.col("contact_month").between(F.lit("2008-05-01").cast("date"), F.lit("2010-11-01").cast("date"))).count(),
    }
