-- The same star schema in Snowflake SQL, for comparison with ../sql/01_schema.sql (PostgreSQL).
-- Reference only: this portfolio runs on free local tools, so this file is not executed by the tests.
--
-- What changes in Snowflake:
--   * PRIMARY KEY / FOREIGN KEY / UNIQUE are accepted but NOT ENFORCED (only NOT NULL is).
--     Uniqueness and referential integrity must be tested (dbt tests do exactly that, see 04).
--   * No indexes. Micro-partitions with min/max metadata are pruned automatically; on very large
--     tables a CLUSTER BY key keeps related rows in the same micro-partitions.
--   * IDENTITY / AUTOINCREMENT for surrogate keys; TIMESTAMP_NTZ for wall-clock timestamps.

CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key     NUMBER(8)   NOT NULL PRIMARY KEY,
    date         DATE        NOT NULL UNIQUE,
    year         NUMBER(4)   NOT NULL,
    quarter      NUMBER(1)   NOT NULL,
    month        NUMBER(2)   NOT NULL,
    month_name   VARCHAR     NOT NULL,
    day_of_month NUMBER(2)   NOT NULL,
    iso_weekday  NUMBER(1)   NOT NULL,
    weekday_name VARCHAR     NOT NULL,
    is_weekend   BOOLEAN     NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_country (
    country_key NUMBER IDENTITY PRIMARY KEY,
    country     VARCHAR NOT NULL UNIQUE,
    region      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key NUMBER IDENTITY PRIMARY KEY,
    customer_id  NUMBER,
    country_key  NUMBER NOT NULL REFERENCES dw.dim_country (country_key),
    valid_from   DATE   NOT NULL,
    valid_to     DATE   NOT NULL DEFAULT '9999-12-31'::DATE,
    is_current   BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key NUMBER IDENTITY PRIMARY KEY,
    stock_code  VARCHAR NOT NULL UNIQUE,
    description VARCHAR NOT NULL,
    is_product  BOOLEAN NOT NULL,
    updated_at  TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_line_id   NUMBER        NOT NULL PRIMARY KEY,
    invoice_no      VARCHAR       NOT NULL,
    date_key        NUMBER(8)     NOT NULL REFERENCES dw.dim_date (date_key),
    customer_key    NUMBER        NOT NULL REFERENCES dw.dim_customer (customer_key),
    product_key     NUMBER        NOT NULL REFERENCES dw.dim_product (product_key),
    invoiced_at     TIMESTAMP_NTZ NOT NULL,
    quantity        NUMBER        NOT NULL,
    unit_price      NUMBER(10, 2) NOT NULL,
    revenue         NUMBER(12, 2) NOT NULL,
    line_type       VARCHAR       NOT NULL   -- sale / cancellation / stock_adjustment
)
CLUSTER BY (date_key);   -- most queries filter on a date range
