{{
  config(
    materialized='incremental',
    unique_key='sales_line_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
    contract={'enforced': true},
    indexes=[{'columns': ['date_key']}, {'columns': ['customer_version_key']}]
  )
}}
-- Sales fact, grain = one invoice line (rejected lines excluded). Incremental: a normal run only
-- processes batches from (latest loaded batch - lookback_days) onwards and replaces those lines.
with lines as (
    select * from {{ ref('stg_shop__sales_lines') }}
    where not is_rejected
    {% if is_incremental() %}
      and batch_date >= (select max(batch_date) - {{ var('lookback_days') }} from {{ this }})
    {% endif %}
)

select
    l.sales_line_id::bigint                    as sales_line_id,
    l.invoice_no::text                         as invoice_no,
    to_char(l.invoice_date, 'YYYYMMDD')::int   as date_key,
    v.customer_version_key::text               as customer_version_key,
    l.customer_id::int                         as customer_id,
    l.stock_code::text                         as stock_code,
    l.country::text                            as country,
    l.invoiced_at::timestamp                   as invoiced_at,
    l.batch_date::date                         as batch_date,
    l.quantity::int                            as quantity,
    l.unit_price::numeric(10, 2)               as unit_price,
    l.revenue::numeric(12, 2)                  as revenue,
    l.line_type::text                          as line_type,
    l.is_guest::boolean                        as is_guest
from lines l
-- Point-in-time join: the customer version valid on the day of the sale (guests have none).
left join {{ ref('int_customer_country_versions') }} v
  on v.customer_id = l.customer_id
 and l.invoice_date between v.valid_from and v.valid_to
