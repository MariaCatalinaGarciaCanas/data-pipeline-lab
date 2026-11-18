{#
  Por defecto dbt llama a los esquemas personalizados `<esquema_base>_<personalizado>`
  (p. ej. `main_staging`). Aqui se usa el nombre tal cual (`staging`, `intermediate`, `marts`)
  para que las consultas de DuckDB sean mas legibles.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
