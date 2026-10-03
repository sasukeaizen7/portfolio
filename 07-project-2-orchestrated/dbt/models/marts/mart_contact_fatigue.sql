-- Does calling the same person again help? Conversion by number of calls in the campaign (capped at 10+),
-- with the marginal conversion of each extra call.
with bands as (
    select least(contacts_this_campaign, 10) as calls_in_campaign, converted
    from {{ ref('stg_contacts') }}
)
select
    case when calls_in_campaign = 10 then '10+' else calls_in_campaign::text end  as calls_in_campaign,
    count(*)                                                                       as customers,
    round(100.0 * avg(converted::int), 2)                                          as conversion_pct,
    round(100.0 * avg(converted::int)
          - lag(100.0 * avg(converted::int)) over (order by calls_in_campaign), 2) as change_vs_one_call_fewer_pp
from bands
group by calls_in_campaign
order by min(calls_in_campaign)
