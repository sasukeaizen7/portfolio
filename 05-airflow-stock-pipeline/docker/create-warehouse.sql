-- Runs once, when the Postgres volume is first created: a separate database and user for the data
-- the DAGs load, so it never mixes with Airflow's own metadata.
CREATE USER de WITH PASSWORD 'de';
CREATE DATABASE warehouse OWNER de;
