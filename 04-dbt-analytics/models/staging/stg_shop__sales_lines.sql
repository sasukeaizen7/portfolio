-- One row per source line: renamed, typed, and flagged. No rows are removed here; staging models
-- describe the source, marts decide what to keep.
with source as (
    select * from {{ source('shop', 'sales_lines') }}
)

select
    line_id                                         as sales_line_id,
    batch_date,
    invoice                                         as invoice_no,
    stock_code,
    description                                     as product_description,
    quantity,
    invoiced_at,
    invoiced_at::date                               as invoice_date,
    unit_price,
    {{ money('quantity * unit_price') }}            as revenue,
    customer_id,
    country,
    -- Only invoices starting with C are cancellations. Negative quantities on normal invoices are stock
    -- adjustments (damages, write-offs) at price 0: they move stock, not money.
    case when invoice like 'C%' then 'cancellation'
         when quantity < 0      then 'stock_adjustment'
         else 'sale' end                            as line_type,
    stock_code ~ '^[0-9]{5}'                        as is_product,
    customer_id is null                             as is_guest,
    -- The same rejection rule as Project 1: these lines can't be facts.
    quantity = 0 or unit_price < 0                  as is_rejected,
    loaded_at
from source
