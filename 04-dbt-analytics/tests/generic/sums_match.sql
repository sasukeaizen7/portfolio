{#
  Custom generic test: the sum of `column_name` in this model equals the sum of `compare_column` in
  `compare_model` (optionally filtered). Reconciliation between layers catches silently dropped or
  duplicated rows that row-level tests miss. Fails (returns a row) if the totals differ.
#}
{% test sums_match(model, column_name, compare_model, compare_column, compare_where='true') %}
with a as (select coalesce(sum({{ column_name }}), 0) as total from {{ model }}),
     b as (select coalesce(sum({{ compare_column }}), 0) as total from {{ compare_model }} where {{ compare_where }})
select a.total as model_total, b.total as compare_total
from a cross join b
where a.total <> b.total
{% endtest %}
