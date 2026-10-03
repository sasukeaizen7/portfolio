-- bikes + free docks should not exceed capacity (+2 tolerance for bikes parked in overflow). A warning,
-- not an error: the feed itself is occasionally inconsistent, which is worth seeing, not blocking on.
{{ config(severity='warn') }}
select f.snapshot_id, f.station_id, f.bikes, f.docks, st.capacity
from {{ ref('fct_station_snapshots') }} f
join {{ ref('stg_velib__stations') }} st using (station_id)
where f.bikes + f.docks > st.capacity + 2
