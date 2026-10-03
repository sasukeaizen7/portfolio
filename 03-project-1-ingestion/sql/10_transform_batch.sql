-- Raw -> star schema for ONE batch date. Parameter: %(batch_date)s. Runs inside the batch's
-- transaction, after the raw lines of that date were (re)inserted. Every step is idempotent.

-- 1. Customers: the latest country of each identified customer in this batch -> SCD 2 merge
--    (the merge itself is 02-data-modeling/sql/03_scd2_merge.sql, run right after this file's step 1).
TRUNCATE stage.customer_snapshot;
INSERT INTO stage.customer_snapshot (customer_id, country, as_of)
SELECT DISTINCT ON (customer_id) customer_id, country, %(batch_date)s::date
FROM raw.sales_lines
WHERE batch_date = %(batch_date)s AND customer_id IS NOT NULL
ORDER BY customer_id, invoiced_at DESC, line_id DESC;
