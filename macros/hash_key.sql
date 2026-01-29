{% macro hash_key(columns) %}
sha256(
  to_utf8(
    concat_ws(
      '||',
      {% for col in columns %}
        coalesce(cast({{ col }} as varchar), ''){% if not loop.last %},{% endif %}
      {% endfor %}
    )
  )
)
{% endmacro %}