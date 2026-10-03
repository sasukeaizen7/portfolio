-- Monthly KPIs by region: gross sales, cancellations, net revenue, orders, active customers,
-- and month-over-month growth of net revenue.
with monthly as (
    select
        date_trunc('month', f.invoiced_at)::date                     as month,
        coalesce(r.region, 'Unknown')                                as region,
        sum(f.revenue) filter (where f.line_type = 'sale')           as gross_sales,
        -sum(f.revenue) filter (where f.line_type = 'cancellation')  as cancellations,
        sum(f.revenue)                                               as net_revenue,
        count(distinct f.invoice_no) filter (where f.line_type = 'sale') as orders,
        count(distinct f.customer_id)                                as active_customers
    from {{ ref('fct_sales') }} f
    left join {{ ref('country_regions') }} r using (country)
    group by 1, 2
)

select
    *,
    round(100.0 * (net_revenue / nullif(lag(net_revenue) over (partition by region order by month), 0) - 1), 1)
        as net_revenue_mom_pct
from monthly
