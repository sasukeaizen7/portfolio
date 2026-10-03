-- An analysis: compiled by dbt (refs resolved) but never materialized. Run `dbt compile`, then the SQL
-- in target/compiled/ from any client. Top 5 products by net revenue in each region.
with ranked as (
    select r.region, p.description, sum(f.revenue) as net_revenue,
           rank() over (partition by r.region order by sum(f.revenue) desc) as rank_in_region
    from {{ ref('fct_sales') }} f
    join {{ ref('dim_products') }} p using (stock_code)
    left join {{ ref('country_regions') }} r using (country)
    where p.is_product
    group by r.region, p.description
)

select * from ranked where rank_in_region <= 5 order by region, rank_in_region
