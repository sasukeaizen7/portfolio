# Learning notes: 01 · Python ETL

Read this alongside the code. Each section answers a question an interviewer is likely to ask.

## Why these design choices
**Why retry 429/5xx but not 4xx?** A 503 or a reset connection is *transient*: the same request may work a second later. A 400 means the request itself is wrong, so retrying wastes time and hides the bug. Backoff doubles the wait (1 s, 2 s, 4 s) so a struggling server isn't hammered.

**Why validate in `transform` and not in the database?** Both, in fact: the Postgres table also has `CHECK` constraints. Validating in Python gives a clear error for the whole batch *before* anything is written. The constraints are the last line of defense if someone loads data by another path.

**Why are nulls allowed?** The API returns `null` for days it has no data. Rejecting the whole batch for one missing value would be worse than storing `NULL` and measuring how often it happens.

**What makes the load idempotent?** In Parquet, each run rewrites the partitions it touches (`city=X/month=Y`). In Postgres, rows go into a temp table with `COPY` (fast), then `INSERT … ON CONFLICT (city, date) DO UPDATE` merges them. Running the same day twice updates instead of duplicating. That makes **retries and backfills safe**, which is the property orchestrators like Airflow rely on.

**Why one transaction?** `with psycopg.connect(...)` commits only if every step succeeds. A crash halfway leaves the table as it was, never half-loaded.

**Why Hive-style partitions (`city=Paris/month=2024-01`)?** Spark, DuckDB, Athena and pandas read the folder names back as columns, and queries that filter on a partition skip the other folders entirely.

**Why `depends_on: condition: service_healthy`?** A container being *started* doesn't mean Postgres *accepts connections* yet. The health check runs `pg_isready` until it does.

## Things to try (exercises)
1. Add a sixth city to `CITIES` and backfill only that city: `--cities nice --start 2024-01-01 --end 2024-12-31 --postgres`.
2. Add the `sunshine_duration` variable end to end: extract, rename, validate (`>= 0`), and a new table column.
3. Make the Postgres load **incremental**: read `max(date)` per city from the table and only fetch the days after it.
4. Write a test that a `requests.Timeout` is retried.
5. Add a `--dry-run` flag that runs extract + transform and prints a summary without writing anything.
6. Query the Parquet lake with DuckDB: `SELECT city, avg(temp_max_c) FROM 'data/**/*.parquet' GROUP BY ALL` (with `hive_partitioning = true`).

## Interview questions this project answers
- "How do you make a pipeline safe to re-run?" → idempotent upserts, partition overwrite.
- "How do you handle an unreliable API?" → timeouts, bounded retries, backoff, retry only transient errors.
- "How do you test code that calls an API?" → inject the session and replay recorded responses.
- "Parquet vs CSV?" → columnar, typed, compressed, splittable, with partition pruning.
