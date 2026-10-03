"""Shared setup: the SparkSession, paths, timing."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(os.environ.get("APP_ROOT", "/app"))
DATA = ROOT / "data"
RAW = DATA / "raw"
CLEAN = DATA / "clean"
RESULTS = ROOT / "results"


def get_spark(app_name: str, **conf: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")          # AQE: re-plans using runtime statistics
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    )
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def timed(label: str, sink: dict | None = None):
    start = time.perf_counter()
    yield
    seconds = time.perf_counter() - start
    print(f"  {label}: {seconds:.2f}s")
    if sink is not None:
        sink[label] = round(seconds, 2)
