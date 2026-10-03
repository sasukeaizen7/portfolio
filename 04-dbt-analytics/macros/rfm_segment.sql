{# RFM scores -> named segment. Kept in a macro so the rule is defined once and unit-testable. #}
{% macro rfm_segment(r, f, m) -%}
    case
        when {{ r }} >= 4 and {{ f }} >= 4 and {{ m }} >= 4 then 'Champions'
        when {{ r }} >= 3 and {{ f }} >= 3                  then 'Loyal'
        when {{ r }} >= 4 and {{ f }} <= 2                  then 'New / promising'
        when {{ r }} <= 2 and {{ f }} >= 3                  then 'At risk'
        when {{ r }} <= 2 and {{ f }} <= 2                  then 'Lost'
        else                                                     'Need attention'
    end
{%- endmacro %}
