# Learning notes: 03 · Project 1

## The questions this project makes you answer
**Full load or incremental?** Incremental by day: a batch only touches its own `batch_date`. A full reload would be simpler but gets slower every day, and couldn't model *history* (SCD 2 needs to see changes in order).

**How is a batch idempotent?**
- `raw`: `DELETE WHERE batch_date = D`, then `COPY`.
- Facts: `DELETE WHERE date_key = D`, then `INSERT`.
- Dimensions: `INSERT … ON CONFLICT`, plus an SCD 2 merge that's a no-op when nothing changed.

Re-running D therefore converges to the same state. `test_rerunning_an_old_batch_is_idempotent` replays day 1 *after* day 2 and compares every fact row.

**Why the point-in-time join?** When an old day is re-run after newer days, the customer's *current* version may be a later one. Joining on `invoiced_at::date BETWEEN valid_from AND valid_to` picks the version that was true when the sale happened.

**Why one transaction per batch?** Without it, a crash between "insert raw" and "insert facts" would leave a half-loaded day. With it, a day is either fully in or not at all.

**Why are the audit rows written outside the transaction?** If the batch rolls back, a log row written inside it would roll back too, and you'd lose the evidence of the failure. `run_batch` inserts the `running` row first (autocommit), then updates it to `failed` or `success`.

**What do the quality checks catch?** `test_quality_check_failure_blocks_the_batch` sabotages the fact SQL (it drops guest lines). The "every raw line is loaded or rejected" and "revenue reconciles" checks fail, and the day is not committed. Reconciliation checks (counts and sums between layers) catch most silent pipeline bugs.

**Why convert Excel to Parquet first ("landing")?** Parsing Excel is slow and type-ambiguous. One conversion gives a typed, columnar, compressed file. Source quirks (the duplicated sheet overlap, Excel serial dates) are fixed in exactly one place, before the warehouse.

**Why load the feed into an in-memory DuckDB table?** Profiling showed that scanning the Parquet file for each of 604 days took ~1 s each, while the database work took 0.1 s. Load once, query per day: 93 s total instead of ~13 min. *Measure before optimizing.*

**Why do some customers have 5–7 versions?** The source records the country per *invoice*. Customer 12422's invoices alternate between Australia and Switzerland. SCD 2 records exactly what the source says. Whether that's a real move or a data-entry habit is a question to raise with the business, not something to silently "fix".

## Exercises
1. Add a 7th check: no fact line has a `date_key` outside its batch date.
2. Make the pipeline skip days already loaded successfully, unless `--force` is given (use `audit.load_runs`).
3. Add `dim_product` history (SCD 2) for price changes, and a query showing a product's price over time.
4. Replace `DELETE + INSERT` on facts with `MERGE` (PostgreSQL 15+) and compare the run time.
5. Write the 3 queries a sales manager would ask on Monday morning, using only `dw.*`.
6. Draw how this would run in the cloud: S3 landing, a scheduled job, a managed Postgres or Snowflake. (Project 2 and the final project do exactly that with local stand-ins.)

## Interview pitch (2 minutes)
"I built a daily batch pipeline that loads two years of a real retailer's sales into a star schema in Postgres, one day at a time, in Docker. Each day is one transaction and is idempotent, so I can re-run any day safely. The customer dimension keeps history with SCD type 2, and each sale joins the version valid on its date. Six data-quality checks reconcile counts and revenue between layers. If one fails, the day rolls back and the failure is logged in an audit table. A full backfill of 1 million lines over 604 days takes about 90 seconds, and the tests include a deliberately broken load that the checks catch."

## A bug the dbt project caught (a good interview story)
The first version flagged a line as a cancellation when its quantity was negative. When the dbt project (04) defined cancellations by the invoice number instead ("starts with C", as the source documents), its test "the cancellation flag matches the sign of the quantity" failed on 3,394 lines. One was a C invoice with a positive quantity. The other **3,393 were not cancellations at all**: negative quantities on normal invoices, at price 0, with descriptions like "damages", "check" and "missing". These are **stock adjustments**, not cancellations. The fix replaced the boolean with `line_type` (`sale` / `cancellation` / `stock_adjustment`) in the schema, the pipeline and the dbt models. Cancellations went from 22,557 to the true 19,165.

*Lesson:* a second, independent implementation plus tests that compare the two is one of the best ways to find wrong assumptions.
