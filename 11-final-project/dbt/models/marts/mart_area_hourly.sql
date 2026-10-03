-- Average fill and share of empty stations by area and Paris-local hour of day.
select
    st.area,
    coalesce(st.arrondissement, 99)                                     as area_order,   -- 1er..20e, then suburbs
    extract(hour from f.taken_at_paris)::int                            as hour,
    count(distinct f.snapshot_id)                                       as snapshots,
    round(avg(f.fill_pct), 1)                                           as avg_fill_pct,
    round(100.0 * avg(f.is_empty::int), 1)                              as empty_pct
from {{ ref('fct_station_snapshots') }} f
join {{ ref('stg_velib__stations') }} st using (station_id)
where f.is_operating
group by st.area, coalesce(st.arrondissement, 99), extract(hour from f.taken_at_paris)
