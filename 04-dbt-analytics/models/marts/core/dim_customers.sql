-- Customer dimension, SCD 2, flattened for BI tools: region and ISO code are copied onto each version
-- (Project 1 snowflakes them out; a mart optimizes for easy querying instead).
select
    v.customer_version_key,
    v.customer_id,
    v.version_number,
    v.country,
    r.region,
    r.iso_code,
    v.valid_from,
    v.valid_to,
    v.is_current
from {{ ref('int_customer_country_versions') }} v
left join {{ ref('country_regions') }} r using (country)
