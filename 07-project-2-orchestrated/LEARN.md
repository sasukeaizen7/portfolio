# Learning notes: 07 · Project 2

## Design questions
**Why Spark for 41k rows?** Honestly: not for speed. pandas would be faster at this size. The point is the *pattern*: a Spark job submitted by Airflow, with arguments, that writes partitioned Parquet. Swap the file for 400M rows and the same job and DAG still work. Say this in interviews; it shows you know when Spark is and isn't needed.

**Why does the year derivation need one partition?** "Every time the month goes down, add a year" depends on row order. Spark doesn't keep file order across partitions, so the job `coalesce(1)`s before numbering rows and uses a window without `partitionBy`. Spark warns that this moves all data to one task, which is fine for 41k rows. At scale you'd fix the source instead, by asking for a real date column.

**How does Airflow run Spark here?** `SparkSubmitOperator` with the `spark_local` connection (`local[*]`), so Spark runs inside the task's container. Pointing the connection at a cluster (`spark://host:7077`, YARN, Kubernetes) runs the same job distributed, without changing the DAG.

**Why is dbt in its own virtualenv?** dbt and Airflow pin different versions of shared libraries. Installing both in one environment is the classic dependency conflict. A separate venv, called by a `BashOperator`, avoids it. Astronomer Cosmos is the more elaborate option: it turns each dbt model into an Airflow task.

**Why TRUNCATE + COPY for the load?** The whole source is re-delivered each run and the table is small. A full reload in one transaction is the simplest idempotent strategy. Project 1 shows the incremental alternative.

**What is target leakage?** Using information that isn't available at prediction time. Call `duration` is known only after the call, and long calls are the successful ones. The staging model drops it, and documents why.

## Exercises
1. Add a `ShortCircuitOperator` (or `@task.short_circuit`) that skips the Spark job when the landed file's checksum hasn't changed since the last run.
2. Point `spark_local` at the standalone cluster from project 06 and run the job distributed. Watch out for the Python version, which must match on driver and executors.
3. Add a dbt model `mart_campaign_roi` with a seed of assumed cost per call and revenue per conversion.
4. Replace the BashOperator with Astronomer Cosmos and compare the graph view.
5. Add an `on_failure_callback` to the DAG that records failed runs in a table.

## Interview pitch
"Project 2 is an Airflow DAG that lands a public marketing dataset, runs a Spark job to clean and partition it, loads Postgres and runs dbt models and tests, then publishes an Asset. The data had a real problem: no year column. The Spark job reconstructs it from row order, and the result matches the documented date range exactly. The dbt marts show that every extra call to the same customer lowers conversion, and that people who said yes before are 5.8 times more likely to say yes again. I also dropped the call-duration column on purpose, because it's target leakage."
