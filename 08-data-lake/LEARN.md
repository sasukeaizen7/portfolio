# Learning notes: 08 · Data lake (AWS concepts)

## The services, in one sentence each
- **S3**: object storage. Keys look like paths, but they're just strings, and "folders" are key prefixes.
- **Glue job**: managed Spark (or Python shell) that transforms data between zones.
- **Glue crawler / Data Catalog**: discovers schemas and partitions and stores table definitions that Athena, Redshift Spectrum and EMR all share.
- **Athena**: serverless SQL (Trino) over S3, billed per TB scanned, which is why partitioning and Parquet matter financially.
- **IAM**: who can do what on which resource. Policies are JSON documents of `Effect / Action / Resource`.
- **Lambda**: functions triggered by events (e.g. an S3 `PutObject`). It's the classic way to kick off processing when a file lands.
- **Kinesis**: managed streaming (like Kafka, see project 09). Firehose writes streams into S3 in batches.
- **Redshift**: the AWS data warehouse. Spectrum lets it query the lake directly.

## Questions you should be able to answer
**Why partition raw by ingestion date but clean by event date?** Raw answers "what did we receive, and when?", which you need for audits and replays. Clean answers business questions, which filter on *when it happened*. A file received on 3 October can contain June rides.

**What does partition pruning save?** Athena bills by bytes scanned. A query on `month IN (7, 8)` reads 2 of 12 folders, roughly 1/6 of the cost and time. Parquet adds column pruning: a query that touches 3 of 16 columns reads only those 3.

**Why Parquet + zstd?** It's columnar (read only the needed columns), typed (no CSV parsing), compressed, and splittable.

**Why least privilege?** The analyst key can't delete or overwrite the data it reads, and can't see raw data, which may contain personal fields before cleaning. If the key leaks, the blast radius is "read the clean zone".

**Why does the job choose the latest delivery?** Sources re-send corrected files. Keeping every delivery in raw (immutable) and always rebuilding from the latest makes corrections automatic and keeps history.

## Exercises
1. Add a second table to the crawler (e.g. stations: one row per station with its coordinates) and a query that joins it.
2. Write the AWS **Lambda handler** version of `ingest_raw` (an `event` with the S3 key) and a test that calls it with a fake event.
3. Add a **lifecycle rule** in `iam/`-style JSON: move raw objects to Glacier after 90 days. Explain the trade-off.
4. Make the Glue-style job write **several files per month** (e.g. one per day) and measure the effect on the query time.
5. Run the same pipeline on a real AWS free-tier account: create the buckets, set `S3_ENDPOINT` to nothing, and use an IAM user built from `iam/pipeline-writer.json`.

## Interview pitch
"I built a small AWS-style data lake locally: an immutable raw zone partitioned by ingestion date, a Glue-style job that turns zipped CSVs into partitioned Parquet, a crawler that publishes a catalog, and Athena-style SQL through that catalog. Access is least-privilege: a pipeline identity writes, and an analyst identity can only read the clean zone and write results. A script proves the denials. The same code runs on real S3: only the endpoint changes."
