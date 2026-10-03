-- "Athena" queries over the clean zone. Partition columns (year, month) come from the folder names:
-- filtering on them reads only the matching files (partition pruning).

-- name: monthly_rides
-- Rides per month, members vs casual riders, and the share on e-bikes.
SELECT year, month,
       count(*)                                                       AS rides,
       round(100.0 * avg((member_casual = 'member')::int), 1)         AS member_pct,
       round(100.0 * avg((rideable_type = 'electric_bike')::int), 1)  AS ebike_pct,
       round(median(duration_min), 1)                                 AS median_minutes
FROM citibike_trips
GROUP BY year, month
ORDER BY year, month;

-- name: busiest_stations
-- The 10 busiest start stations of the year, with how many rides end where they started.
SELECT start_station_name,
       count(*)                                                       AS rides,
       round(100.0 * avg((end_station_name = start_station_name)::int), 1) AS round_trip_pct
FROM citibike_trips
WHERE start_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY rides DESC, start_station_name
LIMIT 10;

-- name: hourly_profile
-- Rides by hour: weekday commute peaks vs weekend afternoons.
SELECT CASE WHEN isodow(started_at) IN (6, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,
       hour(started_at)                                               AS hour,
       count(*)                                                       AS rides
FROM citibike_trips
GROUP BY ALL
ORDER BY day_type, hour;

-- name: summer_partition_pruning
-- Only July and August: thanks to the month=7 / month=8 folders, the other 10 files are never read.
SELECT member_casual, count(*) AS rides, round(avg(duration_min), 1) AS avg_minutes
FROM citibike_trips
WHERE year = 2024 AND month IN (7, 8)
GROUP BY member_casual
ORDER BY member_casual;
