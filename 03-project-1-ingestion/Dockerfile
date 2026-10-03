# Built from the repository root (see docker-compose.yml): the pipeline applies the star schema
# defined in 02-data-modeling, so both folders go into the image.
FROM python:3.12-slim

WORKDIR /app
COPY 03-project-1-ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY 02-data-modeling/sql/ /app/model-sql/
COPY 03-project-1-ingestion/ingest/ ingest/
COPY 03-project-1-ingestion/sql/ sql/
ENV MODEL_SQL_DIR=/app/model-sql DATA_DIR=/app/data
RUN useradd --create-home etl && mkdir -p /app/data && chown etl /app/data
USER etl

ENTRYPOINT ["python", "-m", "ingest.pipeline"]
CMD ["backfill"]
