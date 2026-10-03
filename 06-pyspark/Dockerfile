# Spark 4.2 (Scala 2.13, Java 17) with Python. Runs in local mode by default; the compose file also
# has a small standalone cluster (profile "cluster").
FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PATH="/opt/spark/bin:${PATH}" PYTHONPATH="/app" PYSPARK_PYTHON=python3
WORKDIR /app
COPY jobs/ jobs/
COPY tests/ tests/
RUN mkdir -p /app/data /app/results && chown -R spark /app
USER spark
