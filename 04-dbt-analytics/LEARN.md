# Learning notes: 04 · dbt

## The mental model
dbt does the **T** of ELT. Every model is a `SELECT`. dbt wraps it in `CREATE TABLE/VIEW AS`, works out the dependency order from `ref()` and `source()`, and runs tests on the results. Your job is to write good SELECTs and say what must be true about them.

## Questions you should be able to answer from this project
**Why three layers?**
- *Staging* mirrors the source 1:1 (rename, cast, flag), so every later model starts from clean names.
- *Intermediate* holds reusable building blocks (customer versions, invoices).
- *Marts* are what people query.

Changing a source column then touches one staging model, not twenty marts.

**View or table?** Staging is a view: cheap, always fresh, and only read by the next layer. Marts are tables, because people query them repeatedly.

**How does the incremental model work?** On the first run (or with `--full-refresh`), `fct_sales` is built from everything. On later runs, `is_incremental()` is true and the model only selects batches from `max(batch_date) - 3` onwards. `delete+insert` on `sales_line_id` then replaces those rows. The 3-day look-back re-captures batches that Project 1 re-ran. The trade-off: older corrections need a `--full-refresh`.

**What does the contract add?** Without it, renaming a column in `fct_sales` silently breaks every dashboard. With `contract: {enforced: true}`, dbt checks names and types before building and fails instead.

**Snapshot vs a gaps-and-islands model, two ways to get SCD 2?**
- `int_customer_country_versions` rebuilds the full history from all sales every run. It's correct for the past, but recomputes everything.
- `snap_customers` (dbt snapshot) stores versions *as it observes them* between runs. It's cheap, works when the source keeps only the current value, but it can't recover history from before the first run.

Real projects use snapshots for sources that overwrite their data.

**Why custom tests like `sums_match`?** `unique`/`not_null` check rows. Reconciliations check *totals between layers*, which catch silently dropped or duplicated rows. The singular tests go further and compare dbt with Project 1's hand-written warehouse.

**What are unit tests for?** Data tests check the *data*; unit tests check the *logic* on fixed inputs. `country_change_opens_a_new_version` pins down the SCD 2 rule (France → Germany → France = 3 versions). `rfm_segments_follow_the_rules` pins down tie handling.

**Why override `generate_schema_name`?** By default dbt prefixes custom schemas with the target schema (`analytics_dbt_marts`). The override keeps clean names. Every dbt team ends up having this conversation.

## Exercises
1. Add a `stg_shop__products` model and move product logic out of `dim_products`.
2. Add a model `fct_daily_product_sales` (product × day) as **incremental**, and a test that its total equals `fct_sales`.
3. Turn `mart_monthly_revenue` into a **dbt metric / semantic model** and query it with MetricFlow.
4. Write a macro `cents(column)` and a unit test for it.
5. Configure `+on_schema_change: append_new_columns` on `fct_sales`, add a column, and watch what happens with and without the contract.
6. Run `dbt source freshness` and explain the result. Then make it fail on purpose.
7. `dbt build --select +mart_customer_rfm` vs `--select mart_customer_rfm+`: explain the difference.

## Interview pitch
"On top of my ingestion project I built a dbt project: sources with freshness checks, staging, intermediate and mart layers, an incremental fact table with an enforced contract, a snapshot, macros and dbt_utils. It has 56 tests, including YAML unit tests and a custom reconciliation test. The best part is that its tests cross-check my hand-written pipeline. The first time I ran it, they disagreed on what a cancellation is, and digging in showed that 3,393 'cancellations' were really stock write-offs. I fixed the model in both projects."
