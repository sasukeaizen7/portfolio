# Spark 4.2 + Delta Lake 4.4 (the open-source table format Databricks is built on).
FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PATH="/opt/spark/bin:${PATH}" PYTHONPATH="/app" PYSPARK_PYTHON=python3 HOME=/home/spark
WORKDIR /app
RUN mkdir -p /home/spark /app/data && chown -R spark /home/spark /app
USER spark
# Download the Delta jars once at build time (into ~/.ivy2), so runs work offline and start fast.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"
COPY --chown=spark medallion/ medallion/
COPY --chown=spark tests/ tests/
