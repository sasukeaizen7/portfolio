import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # One small local session for all tests: 2 threads, few shuffle partitions -> fast.
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()
