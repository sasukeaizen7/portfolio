-- Gaps and islands: consecutive snapshots in which a station was empty form one "empty episode".
-- Snapshots are numbered network-wide; within a station's empty rows, (snapshot number - row number)
-- stays constant along an unbroken run.
with numbered as (
    select f.*, dense_rank() over (order by f.taken_at) as snapshot_number
    from {{ ref('fct_station_snapshots') }} f
    where f.is_operating
),

empty_rows as (
    select station_id, taken_at, snapshot_number,
           snapshot_number - row_number() over (partition by station_id order by snapshot_number) as island
    from numbered
    where is_empty
)

select
    station_id,
    min(taken_at)                                                        as empty_from,
    max(taken_at)                                                        as empty_until,
    count(*)                                                             as snapshots,
    round(extract(epoch from max(taken_at) - min(taken_at)) / 60)::int   as minutes_observed_empty
from empty_rows
group by station_id, island
