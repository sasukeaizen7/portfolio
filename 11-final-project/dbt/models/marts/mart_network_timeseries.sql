-- The whole network at each snapshot: bikes available, e-bike share, share of empty and full stations.
select
    taken_at,
    taken_at_paris,
    sum(bikes)                                                          as bikes,
    sum(ebike)                                                          as ebikes,
    round(100.0 * sum(ebike) / nullif(sum(bikes), 0), 1)                as ebike_share_pct,
    count(*) filter (where is_operating)                                as operating_stations,
    round(100.0 * avg(is_empty::int) filter (where is_operating), 1)    as empty_station_pct,
    round(100.0 * avg(is_full::int) filter (where is_operating), 1)     as full_station_pct
from {{ ref('fct_station_snapshots') }}
group by taken_at, taken_at_paris
