{# Round a money expression to pence, as numeric. One definition used by every model. #}
{% macro money(expression) -%}
    round(({{ expression }})::numeric, 2)
{%- endmacro %}
