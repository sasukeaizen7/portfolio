select
    snapshot_id,
    taken_at,
    (taken_at at time zone 'Europe/Paris')                  as taken_at_paris,
    station_id,
    mechanical,
    ebike,
    mechanical + ebike                                       as bikes,
    docks,
    is_installed,
    is_renting,
    is_returning
from {{ source('raw', 'velib_station_status') }}
