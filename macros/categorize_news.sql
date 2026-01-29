{% macro categorize_news(title) %}
    case
        when lower({{ title }}) like '%crypto%' then 'Crypto'
        when lower({{ title }}) like '%stock%' then 'Stock'
        when lower({{ title }}) like '%market%' then 'Market'
        else 'General'
    end
{% endmacro %}
