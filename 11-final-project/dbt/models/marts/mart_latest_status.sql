-- The latest snapshot of every station, for the dashboard map.
select st.station_id, st.name, st.area, st.lat, st.lon, st.capacity,
       f.taken_at, f.mechanical, f.ebike, f.bikes, f.docks, f.fill_pct, f.is_empty, f.is_full, f.is_operating
from {{ ref('fct_station_snapshots') }} f
join {{ ref('stg_velib__stations') }} st using (station_id)
where f.taken_at = (select max(taken_at) from {{ ref('fct_station_snapshots') }})
