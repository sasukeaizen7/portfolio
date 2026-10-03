-- Dimensions that don't come from the source data. Idempotent.

-- Calendar: every day the business could have traded, generated, not derived from sales.
INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::int,
       d::date,
       extract(year FROM d),
       extract(quarter FROM d),
       extract(month FROM d),
       to_char(d, 'FMMonth'),
       extract(day FROM d),
       extract(isodow FROM d),
       to_char(d, 'FMDay'),
       extract(isodow FROM d) IN (6, 7)
FROM generate_series(DATE '2009-12-01', DATE '2011-12-31', INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- Region rule used by the loaders (one place to change it).
CREATE OR REPLACE FUNCTION dw.region_of(country text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN country = 'United Kingdom' THEN 'United Kingdom'
        WHEN country IN ('Austria', 'Belgium', 'Channel Islands', 'Cyprus', 'Czech Republic', 'Denmark',
                         'EIRE', 'European Community', 'Finland', 'France', 'Germany', 'Greece', 'Iceland',
                         'Italy', 'Lithuania', 'Malta', 'Netherlands', 'Norway', 'Poland', 'Portugal',
                         'Spain', 'Sweden', 'Switzerland') THEN 'Europe'
        ELSE 'Rest of world'
    END
$$;
