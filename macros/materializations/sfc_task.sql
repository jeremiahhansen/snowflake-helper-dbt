{% macro sfc_get_task_node_by_id(node_id) -%}
  {% set node = graph.nodes.values() | selectattr("unique_id", "equalto", node_id) | list | first %}

  {{ return(node) }}
{%- endmacro %}

{% macro sfc_get_task_parent_node(node) -%}
  {% set parent_node = False %}

  {% if node %}
    {% if node.config.materialized != "sfc_task" %}
      {{ exceptions.raise_compiler_error("Current node " ~ node.unique_id ~ " has a materialization defined as " ~ node.config.materialized ~ " (can only be sfc_task)") }}
    {% endif %}

    {% if node.depends_on.nodes|count > 1 %}
      {{ exceptions.raise_compiler_error("Current node " ~ node.unique_id ~ " has " ~ node.depends_on.nodes|count ~ " parent nodes (can only have 1)") }}
    {% endif %}

    {% set parent_node = sfc_helper.sfc_get_task_node_by_id(node.depends_on.nodes[0]) %}
  {% endif %}

  {{ return(parent_node) }}
{%- endmacro %}

{% macro sfc_get_task_top_parent_node(node) -%}
  {#-- Use the namespace() variables so we can set them within the for loop --#}
  {% set ns = namespace(keep_looking=True, parent_node=False) %}

  {#-- Only execute this at run-time and not at parse-time. The model entries in the graph dictionary will be incomplete or incorrect during parsing. --#}
  {% if execute %}
    {% set temp = sfc_helper.sfc_get_task_parent_node(node) %}
    {% if temp %}
      {% set ns.parent_node = temp %}
    {% else %}
      {% set ns.keep_looking = False %}
    {% endif %}

    {#-- While there is still a parent, look it up. There is no while loop in jinja so we need to fake it. --#}
    {% for n in range(100) %}
      {% if ns.keep_looking %}
        {% set temp = sfc_helper.sfc_get_task_parent_node(ns.parent_node) %}
        {% if temp %}
          {% set ns.parent_node = temp %}
        {% else %}
          {% set ns.keep_looking = False %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return(ns.parent_node) }}
{%- endmacro %}

{% macro sfc_create_task_sql(target_relation, sfc_warehouse, sfc_task_schedule, sfc_task_after, sql) -%}
    CREATE OR REPLACE TASK {{ target_relation }}
        WAREHOUSE = {{ sfc_warehouse }}
      {% if sfc_task_schedule %}
        SCHEDULE = '{{ sfc_task_schedule }}'
      {% else %}
        AFTER {{ sfc_task_after }}
      {% endif %}
    AS
        {{ sql }}
    ;
{%- endmacro %}

{% macro sfc_suspend_task(target_relation) -%}
  {% call statement('suspend_task') -%}
    ALTER TASK {{ target_relation }} SUSPEND
  {%- endcall %}
{% endmacro %}

{% macro sfc_resume_task(target_relation) -%}
  {% call statement('resume_task') -%}
    ALTER TASK {{ target_relation }} RESUME
  {%- endcall %}
{% endmacro %}

{% materialization sfc_task, adapter='snowflake' -%}

  {%- set sfc_warehouse = config.get('sfc_warehouse') -%}
  {%- set sfc_task_schedule = config.get('sfc_task_schedule') -%}
  {%- set sfc_task_after = config.get('sfc_task_after') -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- TODO: Update the logic to ALTER existing tasks, not recreate them --#}

  {#-- First, suspend the top parent task if there is one --#}
  {% set top_parent = sfc_helper.sfc_get_task_top_parent_node(model) %}
  {% do log("Found top parent: " ~ top_parent.unique_id, info=true) %}
  {% if top_parent %}
    {% set top_parent_relation = api.Relation.create(database=top_parent.database, schema=top_parent.schema, identifier=top_parent.name) %}
    {% do sfc_helper.sfc_suspend_task(top_parent_relation) %}
  {% endif %}

  {#-- Second, create the new task --#}
  {% set build_sql = sfc_helper.sfc_create_task_sql(target_relation, sfc_warehouse, sfc_task_schedule, sfc_task_after, sql) %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {#-- Third, resume the new task and the top parent task --#}
  {% do sfc_helper.sfc_resume_task(target_relation) %}
  {% if top_parent %}
    {% do sfc_helper.sfc_resume_task(top_parent_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
