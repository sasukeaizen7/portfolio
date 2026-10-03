-- Singular test: the SCD 2 history derived here with gaps-and-islands must match Project 1's
-- incremental SCD 2 merge exactly (same customers, countries and validity dates). Returns the differences.
with dbt_versions as (
    select customer_id, country, valid_from, valid_to from {{ ref('dim_customers') }}
),

project_1 as (
    select cu.customer_id, co.country, cu.valid_from, cu.valid_to
    from {{ source('warehouse', 'dim_customer') }} cu
    join {{ source('warehouse', 'dim_country') }} co using (country_key)
    where cu.customer_id is not null
)

(select 'only in dbt' as side, * from dbt_versions except select 'only in dbt', * from project_1)
union all
(select 'only in project 1', * from project_1 except select 'only in project 1', * from dbt_versions)
