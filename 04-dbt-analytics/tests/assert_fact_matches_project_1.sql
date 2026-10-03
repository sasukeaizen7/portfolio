-- Singular test: dbt's fct_sales and Project 1's dw.fact_sales hold the same lines and revenue.
-- Returns one row (= failure) if counts or totals differ.
with t as (
    select
        (select count(*) from {{ ref('fct_sales') }})                       as dbt_lines,
        (select count(*) from {{ source('warehouse', 'fact_sales') }})      as project_1_lines,
        (select sum(revenue) from {{ ref('fct_sales') }})                   as dbt_revenue,
        (select sum(revenue) from {{ source('warehouse', 'fact_sales') }})  as project_1_revenue
)

select * from t
where dbt_lines <> project_1_lines or dbt_revenue <> project_1_revenue
