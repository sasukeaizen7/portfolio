{# The column, summed, must equal the number of rows in stg_contacts: no call lost or double-counted. #}
{% test sums_to_source_rows(model, column_name) %}
select 1
from (select sum({{ column_name }}) as total from {{ model }}) m
where m.total <> (select count(*) from {{ ref('stg_contacts') }})
{% endtest %}
