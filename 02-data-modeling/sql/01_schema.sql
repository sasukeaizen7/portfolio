-- Star schema for an online gift retailer (UCI Online Retail II), PostgreSQL.
-- Grain of the fact table: ONE INVOICE LINE (one product on one invoice). Every other design choice
-- follows from the grain: what a row means decides which dimensions it can point to.
--
--                     dim_date
--                        |
--   dim_customer --- fact_sales --- dim_product
--     (SCD 2)            |            (SCD 1)
--                     dim_country (via dim_customer, snowflaked on purpose: see LEARN.md)
--
-- Run order: this file, then 02_seed_dimensions.sql. Idempotent (IF NOT EXISTS everywhere).

CREATE SCHEMA IF NOT EXISTS raw;     -- data as received, append-only
CREATE SCHEMA IF NOT EXISTS dw;      -- the star schema
CREATE SCHEMA IF NOT EXISTS audit;   -- load runs and data-quality results

-- ----------------------------------------------------------------------------------------------
-- Dimensions
-- ----------------------------------------------------------------------------------------------

-- A calendar dimension, generated (not derived from facts), so days without sales still exist.
-- Smart key yyyymmdd: readable, sortable, and the fact table stays narrow (int instead of date + attributes).
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key     integer PRIMARY KEY,               -- 20111209
    date         date    NOT NULL UNIQUE,
    year         smallint NOT NULL,
    quarter      smallint NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month        smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name   text    NOT NULL,
    day_of_month smallint NOT NULL,
    iso_weekday  smallint NOT NULL CHECK (iso_weekday BETWEEN 1 AND 7),
    weekday_name text    NOT NULL,
    is_weekend   boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_country (
    country_key  smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country      text NOT NULL UNIQUE,
    region       text NOT NULL                       -- United Kingdom / Europe / Rest of world
);

-- SCD type 2: a customer who moves country gets a NEW row; old sales keep pointing at the old row,
-- so "revenue by country" stays historically correct. valid_to = '9999-12-31' marks the current row.
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key  integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- surrogate key
    customer_id   integer,                                           -- natural (business) key
    country_key   smallint NOT NULL REFERENCES dw.dim_country,
    valid_from    date    NOT NULL,
    valid_to      date    NOT NULL DEFAULT '9999-12-31',
    is_current    boolean NOT NULL DEFAULT true,
    CHECK (valid_to >= valid_from),
    CHECK (is_current = (valid_to = '9999-12-31'))
);
-- At most one current version per customer; versions of a customer never overlap in time
-- is enforced by the loader (03), and checked by a data-quality test.
CREATE UNIQUE INDEX IF NOT EXISTS dim_customer_one_current
    ON dw.dim_customer (customer_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS dim_customer_natural ON dw.dim_customer (customer_id, valid_from);
-- Sales without a customer id (guest checkouts, 21% of lines) point at one "guest" member per country,
-- so revenue by country stays complete: customer_id IS NULL, one row per country, never versioned.
CREATE UNIQUE INDEX IF NOT EXISTS dim_customer_one_guest_per_country
    ON dw.dim_customer (country_key) WHERE customer_id IS NULL;

-- SCD type 1: descriptions get corrected (typos, renames); history isn't worth keeping, so the
-- current description simply overwrites the old one.
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stock_code    text NOT NULL UNIQUE,
    description   text NOT NULL,
    is_product    boolean NOT NULL,                 -- false for postage, fees, manual adjustments
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------------------------
-- Fact
-- ----------------------------------------------------------------------------------------------
-- Additive measures only (quantity, revenue); unit_price is kept for reference but never summed.
-- Three kinds of lines, told apart by line_type: sales; cancellations (invoice number starting with C,
-- negative revenue), so net revenue = sum(revenue); and stock adjustments (negative quantity on a normal
-- invoice at price 0: damages, write-offs), which move stock but no money.
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_line_id   bigint PRIMARY KEY,              -- = raw line id: traceable back to the source
    invoice_no      text     NOT NULL,               -- degenerate dimension (no attributes of its own)
    date_key        integer  NOT NULL REFERENCES dw.dim_date,
    customer_key    integer  NOT NULL REFERENCES dw.dim_customer,
    product_key     integer  NOT NULL REFERENCES dw.dim_product,
    invoiced_at     timestamp NOT NULL,
    quantity        integer  NOT NULL CHECK (quantity <> 0),
    unit_price      numeric(10, 2) NOT NULL CHECK (unit_price >= 0),
    revenue         numeric(12, 2) NOT NULL,
    line_type       text     NOT NULL CHECK (line_type IN ('sale', 'cancellation', 'stock_adjustment')),
    CHECK (line_type <> 'sale' OR quantity > 0),
    CHECK (line_type <> 'stock_adjustment' OR revenue = 0)
);
-- Postgres doesn't index foreign keys: the common filters/joins get one each.
CREATE INDEX IF NOT EXISTS fact_sales_date     ON dw.fact_sales (date_key);
CREATE INDEX IF NOT EXISTS fact_sales_customer ON dw.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS fact_sales_product  ON dw.fact_sales (product_key);
