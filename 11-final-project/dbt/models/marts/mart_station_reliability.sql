-- Per station over the collected period: how often it is empty or full, and its longest empty episode.
with stats as (
    select
        station_id,
        count(*)                                                 as snapshots,
        round(100.0 * avg(is_empty::int), 1)                     as empty_pct,
        round(100.0 * avg(is_full::int), 1)                      as full_pct,
        round(avg(fill_pct), 1)                                  as avg_fill_pct,
        round(100.0 * sum(ebike) / nullif(sum(bikes), 0), 1)     as ebike_share_pct
    from {{ ref('fct_station_snapshots') }}
    where is_operating
    group by station_id
),

longest as (
    select station_id, max(minutes_observed_empty) as longest_empty_minutes
    from {{ ref('mart_empty_episodes') }}
    group by station_id
)

select st.station_id, st.name, st.area, st.capacity, st.lat, st.lon, s.snapshots, s.empty_pct, s.full_pct,
       s.avg_fill_pct, s.ebike_share_pct, coalesce(l.longest_empty_minutes, 0) as longest_empty_minutes
from stats s
join {{ ref('stg_velib__stations') }} st using (station_id)
left join longest l using (station_id)
