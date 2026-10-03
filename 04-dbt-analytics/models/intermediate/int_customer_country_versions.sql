-- SCD 2 history of each customer's country, rebuilt from the FULL sales history (gaps and islands):
-- order each customer's invoices in time, start a new version whenever the country differs from the
-- previous invoice, and close each version the day before the next one starts.
-- Same result as Project 1's incremental merge, but derived in one query (and testable against it).
with invoice_days as (
    -- One country per customer per day: the country of the day's latest line.
    select distinct on (customer_id, invoice_date)
        customer_id, invoice_date, country
    from {{ ref('stg_shop__sales_lines') }}
    where customer_id is not null
    order by customer_id, invoice_date, invoiced_at desc, sales_line_id desc
),

changes as (
    select
        *,
        case when country is distinct from lag(country) over (partition by customer_id order by invoice_date)
             then 1 else 0 end as is_new_version
    from invoice_days
),

numbered as (
    select *, sum(is_new_version) over (partition by customer_id order by invoice_date) as version_number
    from changes
),

versions as (
    select customer_id, version_number, country, min(invoice_date) as valid_from
    from numbered
    group by customer_id, version_number, country
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'version_number']) }} as customer_version_key,
    customer_id,
    version_number,
    country,
    valid_from,
    coalesce(lead(valid_from) over (partition by customer_id order by valid_from) - 1, date '9999-12-31') as valid_to,
    lead(valid_from) over (partition by customer_id order by valid_from) is null                         as is_current
from versions
