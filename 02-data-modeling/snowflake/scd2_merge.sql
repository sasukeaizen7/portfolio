-- SCD 2 in Snowflake with MERGE (reference only, not executed by the tests).
-- The classic trick: the USING clause emits each changed customer TWICE, once with a NULL merge key
-- (falls into WHEN NOT MATCHED -> insert the new version) and once with its real id (WHEN MATCHED ->
-- expire the old version). Brand-new customers appear once, with a NULL key -> inserted.
MERGE INTO dw.dim_customer AS d
USING (
    SELECT s.customer_id AS merge_key, s.customer_id, c.country_key, s.as_of
    FROM stage.customer_snapshot s
    JOIN dw.dim_country c ON c.country = s.country
    UNION ALL
    SELECT NULL, s.customer_id, c.country_key, s.as_of
    FROM stage.customer_snapshot s
    JOIN dw.dim_country c ON c.country = s.country
    JOIN dw.dim_customer cur ON cur.customer_id = s.customer_id AND cur.is_current
    WHERE cur.country_key <> c.country_key
) AS src
ON d.customer_id = src.merge_key AND d.is_current
WHEN MATCHED AND d.country_key <> src.country_key THEN
    UPDATE SET valid_to = DATEADD(day, -1, src.as_of), is_current = FALSE
WHEN NOT MATCHED THEN       -- the NULL-key copy of a changed customer, or a brand-new customer
    INSERT (customer_id, country_key, valid_from) VALUES (src.customer_id, src.country_key, src.as_of);
