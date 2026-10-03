-- Data-quality checks for ONE batch (%(batch_date)s). Each row: check name, passed, observed value.
-- (Literal percent signs are doubled because psycopg reads a single percent sign as a placeholder.)
-- The pipeline stores them in audit.dq_results and fails the batch if any check fails.
WITH b AS (SELECT %(batch_date)s::date AS d, to_char(%(batch_date)s::date, 'YYYYMMDD')::int AS k),
raw AS (
    SELECT count(*)                                                              AS lines,
           count(*) FILTER (WHERE quantity = 0 OR unit_price < 0)                AS rejectable,
           coalesce(sum(round(quantity * unit_price, 2)) FILTER (WHERE quantity <> 0 AND unit_price >= 0), 0) AS revenue
    FROM raw.sales_lines, b WHERE batch_date = b.d
),
fact AS (
    SELECT count(*) AS lines, coalesce(sum(revenue), 0) AS revenue
    FROM dw.fact_sales, b WHERE date_key = b.k
)
SELECT 'every raw line is loaded or rejected', raw.lines = fact.lines + raw.rejectable,
       format('raw %%s = fact %%s + rejected %%s', raw.lines, fact.lines, raw.rejectable)
FROM raw, fact
UNION ALL
SELECT 'revenue reconciles with raw', raw.revenue = fact.revenue,
       format('raw %%s vs fact %%s', raw.revenue, fact.revenue)
FROM raw, fact
UNION ALL
SELECT 'batch is not empty', raw.lines > 0, raw.lines::text FROM raw
UNION ALL
SELECT 'one current version per customer',
       NOT EXISTS (SELECT customer_id FROM dw.dim_customer WHERE customer_id IS NOT NULL AND is_current
                   GROUP BY customer_id HAVING count(*) > 1), NULL
UNION ALL
SELECT 'customer versions never overlap',
       NOT EXISTS (SELECT 1 FROM dw.dim_customer a JOIN dw.dim_customer c
                   ON a.customer_id = c.customer_id AND a.customer_key < c.customer_key
                   AND a.valid_from <= c.valid_to AND c.valid_from <= a.valid_to), NULL
UNION ALL
SELECT 'sales have a positive quantity',
       NOT EXISTS (SELECT 1 FROM dw.fact_sales, b WHERE date_key = b.k AND line_type = 'sale' AND quantity <= 0), NULL;
