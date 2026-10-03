# Learning notes: 11 · Final project

## Design decisions (and the question each one answers)
**Why keep every raw snapshot in S3 before loading?** The API has no history: a snapshot not saved is lost forever. The raw zone is the source of truth. If the warehouse is wiped or a dbt bug is found, everything can be rebuilt from S3. Storage is cheap; lost data isn't recoverable.

**Why are capture and load separate tasks?** So a warehouse outage doesn't lose data. Capture keeps writing to S3, and the next successful load picks up *every* snapshot not yet loaded (it compares the bucket with the load log). The pipeline heals itself without a manual backfill.

**Why schedule the transformation on an Asset?** dbt runs only when new raw data has landed, right after it lands, and never on a failed load.

**Why an incremental fact table?** ~1,500 rows × 144 snapshots a day ≈ 216k rows a day. Rebuilding months of history every 10 minutes would get slower every day. The incremental model only processes the last hour, and `delete+insert` on `(snapshot_id, station_id)` makes overlaps harmless.

**Why both dbt tests and a monitoring DAG?** dbt tests check the data a run *produced*. The monitoring DAG checks things no run can see from the inside: "no snapshot for 30 minutes" means no run happened at all. Freshness is the most important pipeline alert, and it has to come from outside the pipeline.

**Why does an alerting failure not raise?** If the webhook is down, raising would hide the original problem. The alert is always in the table first.

**Why a read-only role for the dashboard?** A BI tool with the owner account can drop tables. With `velib_dashboard` it can only read the marts and the alerts, and it's denied the raw schema (tested).

**Why is the capacity check a warning, not an error?** The feed itself is sometimes inconsistent (bikes + docks > capacity). Blocking the pipeline on the source's own noise would stop everything for something we can't fix. Showing it keeps it honest.

## How it maps to the cloud (be ready to draw this)
- SeaweedFS → **S3**.
- Postgres `warehouse` → **Snowflake**: `COPY INTO` from an S3 stage instead of psycopg `COPY`, plus dbt-snowflake.
- Airflow in Docker → **MWAA** or Astronomer.
- Streamlit → Streamlit in Snowflake, or Tableau/Looker.
- `monitoring.alerts` + webhook → CloudWatch alarms + SNS → Slack.
- Terraform: the same module pattern, with the `snowflake` provider for roles and grants.

## Exercises
1. Add **weather** (Open-Meteo, project 01's extractor) and a mart answering "do empty stations increase when it rains?".
2. Add a **rebalancing** mart: pairs of nearby stations (haversine < 500 m) where one is full and the other empty at the same time.
3. Make the dbt fact **partition-aware** for Snowflake (`cluster_by`) and write the dbt-snowflake profile.
4. Add a `dbt docs` exposure for the dashboard, and a `dbt source freshness` task in the monitoring DAG.
5. Load-test the warehouse: generate a month of snapshots by replaying the collected ones with shifted timestamps, and time `fct_station_snapshots` full vs incremental.

## Interview pitch (2 minutes, practice it in French and English)
"My final project tracks Paris bike-share availability live. Every 10 minutes an Airflow DAG snapshots the Vélib' API (1,500 stations), stores the raw JSON in S3, and loads a warehouse. Loading is idempotent and catches up automatically if a run fails. A second DAG, triggered by a data Asset, runs dbt: staging, an incremental fact table, marts like 'stations most often empty', plus tests, a unit test and source freshness. A third DAG monitors freshness and plausibility and raises alerts to a table and a webhook. The Streamlit dashboard reads the marts through a read-only role created with Terraform, and GitHub Actions tests everything on each push. Building it, a dbt test failed on two stations with 410 docks. They turned out to be temporary event stations at Invalides and Concorde, and now they have their own category."
