# Learning notes: 10 · Delta Lake and the medallion architecture

## What Delta adds to Parquet
A Delta table is Parquet files plus a **transaction log** (`_delta_log/`, one JSON commit per write). The log gives you:
- **ACID transactions:** a write is all or nothing, and readers never see half a write.
- **Versions:** every commit is a version, so you can read `versionAsOf`, inspect `DESCRIBE HISTORY`, or `RESTORE`.
- **MERGE, UPDATE, DELETE** on files that are otherwise immutable.
- **Schema enforcement and evolution.**
- **Change Data Feed:** row-level inserts, updates and deletes between versions.

## Questions you should be able to answer
**Why three layers?**
- *Bronze* is the replayable source of truth: raw, append-only, nothing lost, malformed lines included.
- *Silver* is clean, conformed data, one row per real-world event.
- *Gold* is aggregated for a use case.

When a parsing bug is found, silver and gold are rebuilt from bronze, without going back to the source.

**How does silver stay idempotent?** It uses `MERGE … WHEN NOT MATCHED THEN INSERT` on `event_id`. Re-processing the same bronze rows, or receiving the same event twice, inserts nothing. The test re-delivers a whole day and checks that 0 rows are inserted.

**Why Change Data Feed instead of "read everything every time"?** Bronze grows forever. CDF hands silver only the rows added since the version it last processed (stored in `_pipeline_state`), and silver's CDF hands gold only the dates that changed. That's how incremental pipelines scale.

**Schema evolution vs enforcement?** *Evolution* is accepting a **new column** on purpose: `device` appears in the source and becomes a silver column. *Enforcement* is refusing a **wrong type** for an existing column, which would silently corrupt the table. You want both, and the tests check both.

**What do `OPTIMIZE`, `ZORDER` and `VACUUM` do?**
- `OPTIMIZE` compacts many small files into fewer large ones (small files are slow to list and open).
- `ZORDER BY (user_id)` co-locates rows with similar values, so queries filtering on `user_id` skip more files.
- `VACUUM` deletes files no longer referenced by recent versions. It frees storage, but limits time travel to the retention window, 7 days by default; the demo uses 0.

**Where does Databricks fit?** Databricks adds managed clusters, Unity Catalog (governance), Auto Loader (managed incremental file ingestion), Delta Live Tables / Lakeflow (declarative pipelines with expectations) and Photon. The table format and the patterns here are the same.

## Exercises
1. Add a gold table `user_daily`, built incrementally from silver's CDF.
2. Turn a silver rule into a Databricks-style **expectation**: count violations in a metrics table instead of quarantining.
3. Add `DELETE` support: a GDPR request removes a user from silver. Then propagate it to gold through CDF's `delete` change type.
4. `RESTORE` silver to the version before day 3, re-run, and explain the result.
5. Write bronze to S3 (project 08's SeaweedFS) instead of the local disk with `hadoop-aws`.
