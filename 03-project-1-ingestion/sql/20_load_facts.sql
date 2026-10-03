-- Dimensions that don't need history, then the facts, for ONE batch date (%(batch_date)s).
-- Runs after the SCD 2 merge, in the same transaction.

-- Countries seen only on guest lines.
INSERT INTO dw.dim_country (country, region)
SELECT DISTINCT country, dw.region_of(country)
FROM raw.sales_lines
WHERE batch_date = %(batch_date)s
ON CONFLICT (country) DO NOTHING;

-- One guest member per country (customer_id NULL), created the first time a country has a guest sale.
INSERT INTO dw.dim_customer (customer_id, country_key, valid_from)
SELECT NULL, c.country_key, DATE '2009-12-01'
FROM (SELECT DISTINCT country FROM raw.sales_lines
      WHERE batch_date = %(batch_date)s AND customer_id IS NULL) g
JOIN dw.dim_country c USING (country)
ON CONFLICT (country_key) WHERE customer_id IS NULL DO NOTHING;

-- Products, SCD 1: the latest non-empty description wins. A stock code seen without any description
-- yet gets its code as a placeholder, replaced as soon as a real description arrives.
INSERT INTO dw.dim_product (stock_code, description, is_product)
SELECT DISTINCT ON (stock_code)
       stock_code,
       coalesce(description, stock_code),
       stock_code ~ '^[0-9]{5}'
FROM raw.sales_lines
WHERE batch_date = %(batch_date)s
ORDER BY stock_code, (description IS NULL), invoiced_at DESC, line_id DESC
ON CONFLICT (stock_code) DO UPDATE
SET description = EXCLUDED.description, updated_at = now()
WHERE EXCLUDED.description <> EXCLUDED.stock_code               -- never overwrite with a placeholder
  AND dw.dim_product.description IS DISTINCT FROM EXCLUDED.description;

-- Facts: delete-then-insert the batch's date. Each line finds the customer version valid ON ITS DATE
-- (point-in-time join), so re-running an old date after newer ones is still correct.
-- Lines that can't be facts are rejected and counted by the checks: quantity 0, negative price
-- (the source's "Adjust bad debt" entries).
DELETE FROM dw.fact_sales WHERE date_key = to_char(%(batch_date)s::date, 'YYYYMMDD')::int;

INSERT INTO dw.fact_sales (sales_line_id, invoice_no, date_key, customer_key, product_key, invoiced_at,
                           quantity, unit_price, revenue, line_type)
SELECT r.line_id,
       r.invoice,
       to_char(r.invoiced_at, 'YYYYMMDD')::int,
       cu.customer_key,
       p.product_key,
       r.invoiced_at,
       r.quantity,
       r.unit_price,
       round(r.quantity * r.unit_price, 2),
       CASE WHEN r.invoice LIKE 'C%%' THEN 'cancellation' WHEN r.quantity < 0 THEN 'stock_adjustment' ELSE 'sale' END
FROM raw.sales_lines r
JOIN dw.dim_product p ON p.stock_code = r.stock_code
JOIN dw.dim_country co ON co.country = r.country
JOIN dw.dim_customer cu
  ON (r.customer_id IS NOT NULL AND cu.customer_id = r.customer_id
      AND r.invoiced_at::date BETWEEN cu.valid_from AND cu.valid_to)
  OR (r.customer_id IS NULL AND cu.customer_id IS NULL AND cu.country_key = co.country_key)
WHERE r.batch_date = %(batch_date)s
  AND r.quantity <> 0
  AND r.unit_price >= 0;
