{% materialization raw_sql, default -%}

  {# {% set unique_key = config.get('unique_key') %}
  {% set exclude_columns = (config.require('exclude_columns') | map('upper') | list ) %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(target_relation) %} #}
  {%- set sql_header = config.get('sql_header', none) -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement("main") %}
  {{ sql_header if sql_header is not none }}
      {{ sql }}
  {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': []}) }} -- needed for dbt v1
{%- endmaterialization %}