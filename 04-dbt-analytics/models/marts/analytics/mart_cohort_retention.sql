-- Monthly acquisition cohorts: share of each cohort buying again N months after its first purchase.
with activity as (
    select distinct
        customer_id,
        date_trunc('month', invoiced_at)::date as activity_month
    from {{ ref('fct_sales') }}
    where customer_id is not null and line_type = 'sale'
),

firsts as (
    select customer_id, min(activity_month) as cohort_month from activity group by customer_id
),

cohort_activity as (
    select
        f.cohort_month,
        ((extract(year from a.activity_month) - extract(year from f.cohort_month)) * 12
          + extract(month from a.activity_month) - extract(month from f.cohort_month))::int as months_since_first,
        count(*) as active_customers
    from activity a join firsts f using (customer_id)
    group by 1, 2
)

select
    cohort_month,
    months_since_first,
    active_customers,
    first_value(active_customers) over (partition by cohort_month order by months_since_first) as cohort_size,
    round(100.0 * active_customers
          / first_value(active_customers) over (partition by cohort_month order by months_since_first), 1) as retention_pct
from cohort_activity
