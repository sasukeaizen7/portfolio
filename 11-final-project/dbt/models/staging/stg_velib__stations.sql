-- Paris station codes start with the arrondissement number (16107 -> 16e); codes 21xxx-59xxx and 9xxxx
-- are suburban communes; 60xxx are temporary event megastations (400+ docks, Invalides and Concorde).
-- Checked against the stations' coordinates (see LEARN.md).
with stations as (
    select *, case when station_code ~ '^[0-9]+$' then station_code::int / 1000 end as code_prefix
    from {{ source('raw', 'velib_station_information') }}
)

select
    station_id,
    station_code,
    name,
    lat,
    lon,
    capacity,
    case when code_prefix between 1 and 20 then code_prefix end                      as arrondissement,
    case when code_prefix = 1 then '1er'
         when code_prefix between 2 and 20 then code_prefix::text || 'e'
         when code_prefix = 60 then 'Event stations'
         else 'Suburbs' end                                                           as area
from stations
