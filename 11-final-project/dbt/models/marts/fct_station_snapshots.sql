{{
  config(materialized='incremental', unique_key=['snapshot_id', 'station_id'],
         incremental_strategy='delete+insert', on_schema_change='fail',
         indexes=[{'columns': ['taken_at']}, {'columns': ['station_id']}])
}}
-- One row per station per snapshot. Incremental: each run adds the snapshots loaded since the last one
-- (minus 1 hour, so a snapshot loaded late is still picked up).
select
    s.snapshot_id,
    s.taken_at,
    s.taken_at_paris,
    s.station_id,
    s.mechanical,
    s.ebike,
    s.bikes,
    s.docks,
    round(100.0 * s.bikes / nullif(s.bikes + s.docks, 0), 1)       as fill_pct,
    s.is_renting and s.bikes = 0                                   as is_empty,
    s.is_returning and s.docks = 0                                 as is_full,
    s.is_renting and s.is_returning                                as is_operating
from {{ ref('stg_velib__station_status') }} s
where s.is_installed
{% if is_incremental() %}
  and s.taken_at > (select max(taken_at) - interval '1 hour' from {{ this }})
{% endif %}
