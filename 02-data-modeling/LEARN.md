# Learning notes: 02 · Data modeling

## Core concepts, in this model's terms
**Fact vs dimension.** A *fact* is an event you measure (an invoice line: quantity, revenue). A *dimension* is the context you slice by (when, who, what, where). Facts are long and narrow (keys + numbers); dimensions are short and wide (descriptive text).

**Grain.** Say it before anything else: "one row of `fact_sales` = one product on one invoice." Mixing grains (some rows per line, some per invoice) is the most common modeling bug: sums double count.

**Additive measures.** `quantity` and `revenue` can be summed over any dimension. `unit_price` cannot (summing prices is meaningless), so it is stored for reference only.

**Surrogate vs natural keys.** `customer_id` is the business's key; `customer_key` is ours. SCD 2 needs surrogate keys, because one customer has several rows. Surrogate keys also protect the warehouse when the source reuses or changes its ids.

**Degenerate dimension.** `invoice_no` has no attributes of its own, so it lives in the fact and there's no `dim_invoice` table.

**SCD types.**
- Type 0: never changes (date of birth).
- Type 1: overwrite (product description).
- Type 2: new row per change, with validity dates (customer country).
- Type 3: a "previous value" column (rarely used).

**Star vs snowflake.** A star keeps every dimension one join away from the fact. Snowflaking (country out of customer) adds a join but removes repetition. It's used here deliberately, small and justified, and the dbt marts in 04 flatten it back for BI tools.

## How the SCD 2 merge works (`sql/03_scd2_merge.sql`)
1. Insert countries never seen before.
2. **Expire**: for current rows whose country differs from the batch, set `valid_to = as_of − 1`, `is_current = false`.
3. **Correct**: if the change is dated the day the current version started, overwrite in place (no one-day versions).
4. **Insert**: a new current row for every customer without one (new customers + those just expired).

It is *set-based*: three statements whatever the batch size, no loop. It is *idempotent*: re-running a batch finds nothing to change. The tests prove both.

**Point-in-time join** (how facts find the right version): `fact.invoiced_at::date BETWEEN dim.valid_from AND dim.valid_to`. Project 1 resolves this once, at load time, and stores the surrogate key in the fact.

## Postgres vs Snowflake (`snowflake/`)
- Snowflake **does not enforce** primary, unique or foreign keys. You must *test* them (dbt `unique`, `not_null`, `relationships`).
- No indexes: micro-partition pruning plus an optional `CLUSTER BY`.
- SCD 2 is usually one `MERGE` with the "union the changed rows twice" trick (see the file's comments).

## Exercises
1. Make `dim_product` SCD 2 as well, and explain in two sentences why you probably wouldn't in production.
2. Add a `dim_time_of_day` (hour, part of day) and a query of revenue by part of day.
3. Write the point-in-time query that counts customers per country **as of 2011-01-01**.
4. Design a second fact at a different grain, `fact_daily_product_sales` (product × day), and list which questions it answers faster and which it can't answer at all.
5. Draw this model for a different business (a bike-share: trips, stations, bikes, users) and name the grain.

## Interview questions this project answers
- "Explain a star schema / what's a fact table?" "What's the grain?"
- "How do you track history in a dimension?" → SCD 1 vs 2, with this merge as the example.
- "Why surrogate keys?" "What's a degenerate dimension?"
- "How is modeling in Snowflake different?" → unenforced constraints, no indexes, clustering, MERGE.
