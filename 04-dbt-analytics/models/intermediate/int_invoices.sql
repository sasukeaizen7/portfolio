-- One row per invoice (order): the grain most customer metrics need.
select
    invoice_no,
    min(customer_id)                                            as customer_id,
    min(country)                                                as country,
    min(invoiced_at)                                            as invoiced_at,
    bool_or(line_type = 'cancellation')                         as is_cancellation,
    count(*)                                                    as lines,
    sum(quantity)                                               as units,
    sum(revenue)                                                as revenue
from {{ ref('stg_shop__sales_lines') }}
where not is_rejected and line_type <> 'stock_adjustment'
group by invoice_no
