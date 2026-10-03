import io
import json
import zipfile
from datetime import date

from lake import crawler, ingest_raw, query, raw_to_clean
from lake.s3 import CLEAN_BUCKET

HEADER = ("ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,"
          "end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual")


def trip(ride_id, start="2024-06-03 08:00:00", end="2024-06-03 08:12:00", station="Grove St PATH", member="member"):
    return f"{ride_id},electric_bike,{start},{end},{station},JC1,Hoboken,JC2,40.71,-74.04,40.73,-74.03,{member}"


def zipped(rows):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("JC-202406-citibike-tripdata.csv", "\n".join([HEADER, *rows]) + "\n")
    return buf.getvalue()


GOOD = [trip("A"), trip("B", member="casual"), trip("C", station="City Hall")]
BAD = [trip("D", end="2024-06-03 07:00:00"),                                  # ends before it starts
       trip("E", end="2024-06-05 09:00:00"),                                  # longer than 24 h
       trip("A"),                                                             # duplicate ride id
       trip("F", start="2024-05-31 23:50:00", end="2024-06-01 00:05:00")]    # belongs to May


def test_raw_keys_are_partitioned_by_ingest_date(lake):
    key = ingest_raw.ingest("2024-06", lake, today=date(2026, 10, 3), content=zipped(GOOD))
    assert key == "citibike/jersey_city/ingest_date=2026-10-03/JC-202406-citibike-tripdata.csv.zip"


def test_latest_delivery_wins(lake):
    ingest_raw.ingest("2024-06", lake, today=date(2026, 10, 1), content=zipped(GOOD))
    ingest_raw.ingest("2024-06", lake, today=date(2026, 10, 3), content=zipped(GOOD))
    assert raw_to_clean.latest_raw_files(lake)[("2024", "06")].startswith("citibike/jersey_city/ingest_date=2026-10-03/")


def test_cleaning_rules_and_idempotent_partition_overwrite(lake):
    key = ingest_raw.ingest("2024-06", lake, today=date(2026, 10, 3), content=zipped(GOOD + BAD))
    stats = raw_to_clean.process("2024", "06", key, lake)
    assert (stats["raw_rows"], stats["clean_rows"]) == (7, 3)
    raw_to_clean.process("2024", "06", key, lake)                           # re-run: replaces, never appends
    objects = lake.list_objects_v2(Bucket=CLEAN_BUCKET, Prefix="citibike/trips/")["Contents"]
    assert [o["Key"] for o in objects] == ["citibike/trips/year=2024/month=6/part-0.parquet"]


def test_crawler_catalog_and_queries(lake, tmp_path):
    key = ingest_raw.ingest("2024-06", lake, today=date(2026, 10, 3), content=zipped(GOOD))
    raw_to_clean.process("2024", "06", key, lake)
    catalog = crawler.crawl(lake)
    table = catalog["tables"]["citibike_trips"]
    assert table["partition_keys"] == ["year", "month"] and table["partitions"] == ["year=2024/month=6"]
    assert {"ride_id", "duration_min", "year", "month"} <= {c["name"] for c in table["columns"]}
    assert json.loads(lake.get_object(Bucket=CLEAN_BUCKET, Key="_catalog/catalog.json")["Body"].read()) == catalog

    results = query.run_queries("-- name: by_member\nSELECT member_casual, count(*) AS rides FROM citibike_trips "
                                "GROUP BY 1 ORDER BY 1", tmp_path)
    assert results["by_member"].to_dict("records") == [{"member_casual": "casual", "rides": 1},
                                                      {"member_casual": "member", "rides": 2}]
    assert (tmp_path / "by_member.csv").exists()
