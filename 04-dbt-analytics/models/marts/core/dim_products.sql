-- Product dimension, SCD 1: the latest non-empty description of each stock code.
select distinct on (stock_code)
    stock_code,
    coalesce(product_description, stock_code)  as description,
    is_product,
    min(invoice_date) over (partition by stock_code) as first_sold_on,
    max(invoice_date) over (partition by stock_code) as last_sold_on
from {{ ref('stg_shop__sales_lines') }}
order by stock_code, (product_description is null), invoiced_at desc, sales_line_id desc
