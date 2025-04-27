{% macro dv_generate_source() %}

{{ codegen.generate_source(schema_name='public', table_pattern='src_%', generate_columns=True) }}

{% endmacro %}
