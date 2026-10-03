{#
  dbt's default schema name is "<target schema>_<custom schema>" (analytics_dbt_marts). This override
  uses the custom schema as-is (dbt_marts), so the warehouse reads cleanly next to Project 1's raw/dw/audit.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
