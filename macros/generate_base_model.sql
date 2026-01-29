{% macro generate_base_model(source_name, table_name) %}

{% set source_relation = source(source_name, table_name) %}

{% set columns = adapter.get_columns_in_relation(source_relation) %}

select
    {% for column in columns %}
    {{ column.name | lower }} as {{ column.name | lower }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source_relation }}

{% endmacro %}
