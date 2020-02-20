{% macro sfc_create_task_sql(target_relation, sfc_warehouse, sfc_task_schedule, sql) -%}

    CREATE OR REPLACE TASK {{ target_relation }}
        WAREHOUSE = {{ sfc_warehouse }}
        SCHEDULE = '{{ sfc_task_schedule }}'
    AS
        {{ sql }}
    ;
{%- endmacro %}

{% macro sfc_resume_task(target_relation) -%}
  {% call statement('resume_task') -%}
    ALTER TASK {{ target_relation }} RESUME
  {%- endcall %}
{% endmacro %}

{% materialization sfc_task, adapter='snowflake' -%}

  {%- set sfc_warehouse = config.get('sfc_warehouse') -%}
  {%- set sfc_task_schedule = config.get('sfc_task_schedule') -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set build_sql = sfc_create_task_sql(target_relation, sfc_warehouse, sfc_task_schedule, sql) %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do sfc_resume_task(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
