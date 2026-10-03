# Learning notes: 05 · Airflow

## Airflow 3 architecture (what each container does)
- **api-server**: the UI and REST API, plus the *execution API* that running tasks report to. In Airflow 3, tasks no longer talk to the metadata database directly.
- **scheduler**: decides which task instances should run, and with the LocalExecutor it also runs them as subprocesses.
- **dag-processor**: parses the files in `dags/` and stores the serialized DAGs. It's a separate process in Airflow 3, so a slow or broken DAG file can't stall scheduling.
- **postgres**: the metadata database (DAG runs, task states, XComs), plus a separate `warehouse` database for our data.
- Not used here: **triggerer** (for deferrable operators) and **Celery workers + Redis** (to scale out across machines).

## Questions you should be able to answer
**Why is the logic outside the DAG file?** The DAG processor imports every DAG file every few seconds, so heavy imports or work at the top level slow everything down. Keeping logic in `include/` keeps DAG files thin, and the logic gets tested with plain pytest.

**What goes in an XCom?** Small metadata (paths, ids, counts). XComs are stored in the metadata database, so passing a DataFrame through them is the classic Airflow anti-pattern.

**`poke` vs `reschedule` sensors?** A poke sensor occupies a worker slot while it waits. A reschedule sensor frees the slot between checks. For waits measured in minutes or hours, use reschedule (or a deferrable sensor with a triggerer).

**Why `catchup=False` and a lookback window instead of backfilling run by run?** Price APIs return ranges cheaply, and upserts make overlapping windows harmless. One run with `lookback_days=730` replaces 500 tiny runs. When each interval's data can *only* be fetched per interval (e.g. an hourly file dump), you'd use catchup/backfill and `data_interval_start/end` instead.

**What does the Asset give you over a cron schedule?** The report runs when the data actually changed, right after a successful load. It never runs on stale data after a failed day, and nothing breaks if the load becomes slower.

**What makes a DAG run idempotent here?** The upsert on `(symbol, trade_date)`, raw files written per `run_id`, and the report computed with `INSERT … ON CONFLICT`. Clearing and re-running any task gives the same state.

## Exercises
1. Add a `notify_failure` callback (`on_failure_callback`) that writes failures to a table, or posts to a webhook.
2. Turn `api_available` into a **deferrable** sensor and add the `triggerer` service to the compose file.
3. Group `fetch`/`load` in a `@task_group` and look at the graph view.
4. Add a branch (`@task.branch`): skip the load on a US market holiday (no new bar for any symbol).
5. Write a DAG integrity test: load the DAG folder with `DagBag` inside the container and assert no import errors and the expected tasks.
6. Replace the local `data/raw` folder with MinIO (S3) using `ObjectStoragePath`. Project 08 shows the S3 side.

## Interview pitch
"I built two Airflow 3 DAGs. The first waits for a stock API with a reschedule-mode sensor, fetches 8 symbols in parallel with dynamic task mapping, validates the bars and upserts them into Postgres, then runs data-quality checks. It publishes an Asset, and a second DAG is scheduled on that Asset, so the report refreshes only after a successful load. Each run re-fetches a window of days, so runs are idempotent and a failed day heals itself. The business logic is plain Python with its own unit tests, and the DAGs only orchestrate it."
