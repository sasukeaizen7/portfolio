-- Tables owned by the ingestion pipeline (the star schema itself comes from 02-data-modeling).

-- Raw layer: every source line exactly as received, plus which batch brought it. Append-only per
-- batch (a re-run deletes and re-inserts its own batch_date, nothing else).
CREATE TABLE IF NOT EXISTS raw.sales_lines (
    line_id     bigint PRIMARY KEY,
    batch_date  date        NOT NULL,
    invoice     text        NOT NULL,
    stock_code  text        NOT NULL,
    description text,
    quantity    integer     NOT NULL,
    invoiced_at timestamp   NOT NULL,
    unit_price  numeric(10, 2) NOT NULL,
    customer_id integer,
    country     text        NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sales_lines_batch ON raw.sales_lines (batch_date);

CREATE SCHEMA IF NOT EXISTS stage;
CREATE TABLE IF NOT EXISTS stage.customer_snapshot (customer_id integer, country text, as_of date);

-- One row per batch run: what was loaded, how long it took, did it pass its checks.
CREATE TABLE IF NOT EXISTS audit.load_runs (
    run_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_date    date        NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz,
    status        text        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    raw_lines     integer,
    fact_lines    integer,
    rejected      integer,
    error         text
);

-- One row per data-quality check per run.
CREATE TABLE IF NOT EXISTS audit.dq_results (
    run_id     bigint  NOT NULL REFERENCES audit.load_runs,
    check_name text    NOT NULL,
    passed     boolean NOT NULL,
    observed   text,
    PRIMARY KEY (run_id, check_name)
);
