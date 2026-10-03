# Learning notes: 06 · PySpark

## Mental model
- A DataFrame is a **plan**, not data. Transformations (`select`, `where`, `join`, `groupBy`) only build the plan. **Actions** (`count`, `collect`, `write`) run it.
- Spark splits data into **partitions**, and each one is processed by a **task** on an executor core.
- A **shuffle** (from `groupBy`, `join` or `repartition`) moves rows between executors over the network, and it's the expensive part. Most Spark tuning is about avoiding or shrinking shuffles.
- `explain("formatted")` shows the physical plan. Learn to spot `Exchange` (a shuffle), `BroadcastHashJoin` vs `SortMergeJoin`, and `PartitionFilters` (pruning). The performance lab saves the plan of every variant in `results/plans/`.

## Questions you should be able to answer
**When do you switch from pandas to Spark?** When the data doesn't fit comfortably in one machine's memory, or one machine's CPUs aren't enough. Six months of taxi trips (~20M rows) is around the crossover: pandas struggles, DuckDB still copes, Spark is comfortable and scales further. Below a few GB, Spark's overhead usually isn't worth it.

**What is partition pruning?** When data is stored in `pickup_month=2024-03/` folders, a filter on `pickup_month` makes Spark skip the other folders entirely. Filtering on an expression computed from another column (`date_format(pickup_at)`) can't prune: compare the two plans in the lab.

**Broadcast vs sort-merge join?** Joining a 20M-row table to a 265-row table: a broadcast join sends the small table to every executor, so the big one doesn't move. A sort-merge join shuffles *both* sides by the join key. Spark broadcasts automatically below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default).

**What does AQE do?** Adaptive Query Execution re-plans at runtime using real sizes. It coalesces the default 200 shuffle partitions into fewer, right-sized ones, switches joins to broadcast when one side turns out small, and splits skewed partitions.

**What is skew, and what does salting do?** If most rows share a few keys (busy airport zones), the tasks handling those keys do most of the work while the others wait. Salting adds a random suffix to the hot key on the big side and duplicates the small side once per suffix, which spreads one hot key over many tasks. AQE's skew join does a version of this automatically.

**When should you cache?** When the same DataFrame feeds several actions. Without a cache, each action recomputes it from the source. Unpersist when you're done, since cached data takes executor memory.

**`repartition` vs `coalesce`?** `repartition(n)` does a full shuffle to exactly *n* balanced partitions, or by a column. `coalesce(n)` only merges existing partitions (no shuffle), so it can only reduce their number and may leave them unbalanced.

## Exercises
1. Add the green-taxi files and union them with a `taxi_type` column. Watch the schema differences.
2. Rewrite `top_n_per_group` with Spark SQL and `QUALIFY` (Spark 4), and compare the plans.
3. Run `zone_analytics` on the standalone cluster and find the shuffle stages in the UI (http://localhost:8081, then the application UI).
4. Measure the effect of `spark.sql.shuffle.partitions` = 8 / 200 / 2000 with AQE off.
5. Write the trips as Delta instead of Parquet and run `OPTIMIZE ... ZORDER BY (pickup_zone_id)`. Project 10 shows how.
