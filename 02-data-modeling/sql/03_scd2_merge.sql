-- SCD type 2 merge for dw.dim_customer, in set-based statements (no row-by-row loop).
-- Input: a staging table stage.customer_snapshot(customer_id, country, as_of) holding the latest
-- known country of each customer seen in a batch, as of the batch date.
--
-- 1.  CLOSE the current version of every customer whose country changed (valid_to = day before).
-- 1b. A change on the day the current version started is a correction: overwrite in place.
-- 2.  INSERT a new current version for customers that changed AND for brand-new customers.
-- Run them in one transaction. Re-running the same batch changes nothing (idempotent): after step 2
-- the current row already matches the staging row, so neither statement finds anything to do.

-- Countries first (a new country appears -> new dim_country row).
INSERT INTO dw.dim_country (country, region)
SELECT DISTINCT s.country, dw.region_of(s.country)
FROM stage.customer_snapshot s
ON CONFLICT (country) DO NOTHING;

-- Step 1: expire changed versions.
UPDATE dw.dim_customer d
SET valid_to = s.as_of - 1, is_current = false
FROM stage.customer_snapshot s
JOIN dw.dim_country c ON c.country = s.country
WHERE d.customer_id = s.customer_id
  AND d.is_current
  AND d.country_key <> c.country_key
  AND s.as_of > d.valid_from;          -- never expire a version on the day it started

-- Step 1b: a change dated the same day the current version started is a same-day correction:
-- overwrite that version in place (a one-day-long version would be noise).
UPDATE dw.dim_customer d
SET country_key = c.country_key
FROM stage.customer_snapshot s
JOIN dw.dim_country c ON c.country = s.country
WHERE d.customer_id = s.customer_id
  AND d.is_current
  AND d.country_key <> c.country_key
  AND s.as_of = d.valid_from;

-- Step 2: insert new current versions (new customers + the ones just expired).
INSERT INTO dw.dim_customer (customer_id, country_key, valid_from)
SELECT s.customer_id, c.country_key, s.as_of
FROM stage.customer_snapshot s
JOIN dw.dim_country c ON c.country = s.country
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_customer d WHERE d.customer_id = s.customer_id AND d.is_current);
