# Data Engineering in 100 days: portfolio

Every project built during a 100-day data engineering program, from Python ETL to a live, monitored, end-to-end pipeline. Everything runs **locally and for free** with Docker, using open-source stand-ins for the cloud services (each README says which AWS / Snowflake / Databricks service a component plays).

**Stack:** Python · SQL · PostgreSQL · DuckDB · dbt · Airflow 3 · Spark 4 · Delta Lake · Kafka · S3 (SeaweedFS) · Terraform · Docker · GitHub Actions · Streamlit

## The projects, in the order of the plan
| Days | Project | What it shows |
|---|---|---|
| 1–9 | [SQL portfolio](https://github.com/sasukeaizen7/sql-projects) (separate repo) | 18 SQL analytics projects, constraints, advanced SQL, PostgreSQL index benchmarks |
| 10–21 | [01 · Python ETL](01-python-etl) | API → Parquet → Postgres, retries/backoff, validation, idempotent upserts, pytest, Docker |
| 22–28 | [02 · Data modeling](02-data-modeling) | e-commerce star schema, SCD type 2 merge (tested), Snowflake version |
| 29–35 | [03 · Project 1: ingestion pipeline](03-project-1-ingestion) | 1M real sales lines loaded day by day into the star schema: transactional, idempotent, audited, 6 quality checks |
| 32–42 | [04 · dbt analytics](04-dbt-analytics) | sources, seeds, incremental model with a contract, snapshot, macros, 56 tests incl. unit tests; cross-checks Project 1 (and found a real bug in it) |
| 41–49 | [05 · Airflow stock pipeline](05-airflow-stock-pipeline) | Airflow 3: sensor, dynamic task mapping, XComs, retries, Asset-driven scheduling |
| 50–58 | [06 · PySpark](06-pyspark) | 20M NYC taxi trips: partitioned Parquet, broadcast joins, windows, Spark SQL, a performance lab (pruning, joins, AQE, skew, caching) |
| 59–63 | [07 · Project 2: orchestrated pipeline](07-project-2-orchestrated) | Airflow runs a Spark job then dbt on real bank-marketing data |
| 64–70 | [08 · Data lake](08-data-lake) | S3 zones, a Glue-style job, a catalog crawler, Athena-style SQL, least-privilege access (proven) |
| 71–77 | [09 · Kafka streaming](09-kafka-streaming) | producer → 3-partition topic → consumer group → Postgres; exactly-once results, dead-letter queue |
| 78–84 | [10 · Delta medallion](10-medallion-delta) · [infra (Terraform)](infra) · [CI](.github/workflows/ci.yml) | bronze/silver/gold with MERGE, change data feed, schema evolution, time travel; Terraform roles and grants; CI for every project |
| 85–98 | [11 · Final project: Vélib' Paris](11-final-project) | live API → Airflow → S3 → warehouse → dbt → dashboard, monitoring and alerts, Terraform, CI |

Each project folder has a **README** (what it does, results, how to run it) and a **LEARN.md** (the design decisions explained, exercises, and a 2-minute interview pitch).

## Things the tests caught (the best interview stories)
- **3,393 "cancellations" were stock write-offs.** Project 1 flagged cancellations by negative quantity; the dbt project used the invoice prefix. Their cross-check tests disagreed, and the fix became a three-way `line_type` in both ([03](03-project-1-ingestion/LEARN.md)).
- **Two Vélib' stations with 410 docks** failed a range test: temporary event megastations at Invalides and Concorde, now their own category ([11](11-final-project)).
- **A marketing dataset with no year column**: Spark reconstructs it from row order, matching the documented range exactly ([07](07-project-2-orchestrated)).

## Run anything
Requirements: Docker Desktop (with WSL 2 on Windows) and Python 3.12 for the tests.
```bash
cd 03-project-1-ingestion && docker compose up --build
```
Most projects also run their tests without Docker, against a throwaway local Postgres:
```bash
python scripts/local_postgres.py start
```
```bash
cd 01-python-etl && pip install -r requirements-dev.txt && PG_DSN=postgresql://de:de@localhost:5435/de pytest
```
Every push runs [the CI workflow](.github/workflows/ci.yml): lint, Python tests of 8 projects against Postgres, Spark and Delta tests, 3 dbt builds, Terraform validation, and a check of every compose file.

All projects share port 5435 for Postgres, so run one stack at a time (`docker compose down` before switching).
