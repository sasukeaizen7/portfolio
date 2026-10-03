-- Each customer's country as of their latest line: the input of the snapshot.
select distinct on (customer_id) customer_id, country, invoiced_at as last_seen_at
from {{ ref('stg_shop__sales_lines') }}
where customer_id is not null
order by customer_id, invoiced_at desc, sales_line_id desc
