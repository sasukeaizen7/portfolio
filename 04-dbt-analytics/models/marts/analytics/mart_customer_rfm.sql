-- RFM segmentation of identified customers. Scores 1-5 from percent_rank(), so tied customers always
-- get the same score (ntile() would split ties arbitrarily).
with customers as (
    select
        customer_id,
        max(invoiced_at)                                         as last_purchase_at,
        count(distinct invoice_no) filter (where line_type = 'sale') as orders,
        sum(revenue)                                             as net_revenue
    from {{ ref('fct_sales') }}
    where customer_id is not null
    group by customer_id
    having sum(revenue) > 0
),

scored as (
    select
        *,
        least(5, 1 + floor(5 * percent_rank() over (order by last_purchase_at)))::int as recency_score,
        least(5, 1 + floor(5 * percent_rank() over (order by orders)))::int           as frequency_score,
        least(5, 1 + floor(5 * percent_rank() over (order by net_revenue)))::int      as monetary_score
    from customers
)

select
    *,
    {{ rfm_segment('recency_score', 'frequency_score', 'monetary_score') }} as segment
from scored
