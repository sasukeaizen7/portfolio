-- Queries the star schema was designed for. Every one is "facts joined to the dimensions it needs".

-- 1. Net revenue by region and quarter (cancellations included as negative lines).
SELECT d.year, d.quarter, co.region, sum(f.revenue) AS net_revenue
FROM dw.fact_sales f
JOIN dw.dim_date d      ON d.date_key = f.date_key
JOIN dw.dim_customer cu ON cu.customer_key = f.customer_key
JOIN dw.dim_country co  ON co.country_key = cu.country_key
GROUP BY ROLLUP (d.year, d.quarter), co.region
ORDER BY d.year, d.quarter, co.region;

-- 2. SCD 2 at work: revenue by country AS IT WAS when each sale happened, vs by the customer's
--    CURRENT country. The two differ only for customers who moved.
SELECT co_then.country,
       sum(f.revenue)                                         AS revenue_historical,
       sum(f.revenue) FILTER (WHERE cu.is_current)            AS revenue_from_current_versions
FROM dw.fact_sales f
JOIN dw.dim_customer cu   ON cu.customer_key = f.customer_key
JOIN dw.dim_country co_then ON co_then.country_key = cu.country_key
GROUP BY co_then.country
ORDER BY revenue_historical DESC
LIMIT 10;

-- 3. Point-in-time lookup: which country was customer 12370 in on 2011-06-30?
SELECT cu.customer_id, co.country, cu.valid_from, cu.valid_to
FROM dw.dim_customer cu
JOIN dw.dim_country co USING (country_key)
WHERE cu.customer_id = 12370
  AND DATE '2011-06-30' BETWEEN cu.valid_from AND cu.valid_to;

-- 4. Weekend vs weekday sales (an attribute that lives only in the date dimension).
SELECT d.is_weekend, count(DISTINCT f.invoice_no) AS invoices, sum(f.revenue) AS net_revenue
FROM dw.fact_sales f JOIN dw.dim_date d USING (date_key)
GROUP BY d.is_weekend;
