# 01 · Python ETL: weather API → Parquet → Postgres

*Plan days 10–21: Python for data (APIs, JSON/Parquet, pandas, pytest), then Linux, Git and Docker.*

A small but production-shaped batch ETL. It pulls daily weather for 5 French cities from the free [Open-Meteo](https://open-meteo.com/) archive API, validates it, writes it to a Parquet "lake" partitioned by city and month, and upserts it into Postgres. It runs locally or with `docker compose`.

```
Open-Meteo API ──extract──▶ JSON ──transform──▶ typed DataFrame ──load──┬─▶ data/city=…/month=…/part.parquet
  (retries, backoff,            (schema, business rules,                 └─▶ Postgres daily_weather
   timeouts)                     refuse bad batches)                          (COPY + upsert, idempotent)
```

## What it demonstrates
- **Resilient extraction:** timeouts, exponential backoff on 429/5xx and network errors, and no retry on 4xx (a bad request stays bad).
- **Validation before loading:** schema, duplicate keys, `min ≤ max` temperature, value ranges. A bad batch is refused with a clear message instead of being half-loaded.
- **Idempotent loads:** re-running the same dates gives the same result. Parquet partitions are overwritten, and Postgres uses `COPY` into a temp table, then `INSERT … ON CONFLICT DO UPDATE`, all in one transaction.
- **Tests without the network:** a fake HTTP session replays scripted responses (success, 503, connection reset, 400), and a recorded API response serves as a fixture. The Postgres test proves idempotency against a real database.
- **Docker:** a slim non-root image with cached dependency layers, and compose with a Postgres health check so the ETL starts only when the database is ready.

## Run it
```bash
docker compose up --build                       # one run: last 7 days, all cities -> Postgres
```
```bash
docker compose run --rm etl --start 2024-01-01 --end 2024-12-31 --postgres     # backfill a year
```
Without Docker (Python 3.12, from the repo root):
```bash
python scripts/local_postgres.py start          # local Postgres on :5435
```
```bash
cd 01-python-etl && pip install -r requirements-dev.txt
```
```bash
PG_DSN=postgresql://de:de@localhost:5435/de python -m etl.cli --start 2024-01-01 --end 2024-12-31 --postgres
```
```bash
PG_DSN=postgresql://de:de@localhost:5435/de pytest     # 16 tests; the Postgres one is skipped without PG_DSN
```

A full 2024 backfill loads 1,830 rows (5 cities × 366 days) into 60 Parquet partitions in about 2 seconds.

## Files
```
etl/extract.py     HTTP client: retries, backoff, timeouts
etl/transform.py   JSON -> typed DataFrame + validation rules
etl/load.py        Parquet partitions + idempotent Postgres upsert
etl/cli.py         argparse entry point (dates, cities, --postgres)
tests/             pytest: fake HTTP session, recorded fixture, Postgres idempotency
Dockerfile, docker-compose.yml
```
See [LEARN.md](LEARN.md) for the design choices and exercises.
