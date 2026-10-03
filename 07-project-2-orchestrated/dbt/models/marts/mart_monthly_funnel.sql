-- Calls, conversions and conversion rate per month and channel, with the 3-month EURIBOR as context.
select
    contact_month,
    channel,
    count(*)                                                        as calls,
    count(*) filter (where converted)                               as conversions,
    round(100.0 * avg(converted::int), 1)                           as conversion_pct,
    round(avg(euribor_3m), 3)                                       as avg_euribor_3m
from {{ ref('stg_contacts') }}
group by contact_month, channel
