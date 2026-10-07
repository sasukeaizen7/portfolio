import os
import time

import pytest
from pyspark.sql import SparkSession

# The tests build timestamps from naive datetimes, which PySpark converts with the process's local time zone.
# Pin it to UTC (like the session) so an 09:15 pickup stays hour 9 on any machine.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    # One small local session for all tests: 2 threads, few shuffle partitions -> fast.
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()
