-- Which segments convert better than average? Lift = segment rate / overall rate (segments with 300+ calls).
with base as (select avg(converted::int) as overall_rate from {{ ref('stg_contacts') }}),
segments as (
    select 'job' as dimension, job as segment, converted from {{ ref('stg_contacts') }}
    union all select 'age band', age_band, converted from {{ ref('stg_contacts') }}
    union all select 'previous outcome', previous_outcome, converted from {{ ref('stg_contacts') }}
    union all select 'channel', channel, converted from {{ ref('stg_contacts') }}
)
select
    dimension,
    segment,
    count(*)                                                          as calls,
    round(100.0 * avg(converted::int), 1)                             as conversion_pct,
    round(avg(converted::int) / max(base.overall_rate), 2)            as lift
from segments cross join base
group by dimension, segment
having count(*) >= 300
