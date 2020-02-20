{% macro sfc_get_stream_name(table_name) -%}
    {{ return(table_name + '_STREAM') }}
{%- endmacro %}

{% macro sfc_get_create_stream_ddl(schema_name, table_name) -%}
    {%- set stream_name = sfc_get_stream_name(table_name) -%}
    {%- set stream_relation = api.Relation.create(schema=schema_name, identifier=stream_name) %}
    {%- set table_relation = api.Relation.create(schema=schema_name, identifier=table_name) %}

    CREATE STREAM IF NOT EXISTS {{ stream_relation }} ON TABLE {{ table_relation }}
{%- endmacro %}

{% macro sfc_get_stream_metadata_columns(alias) -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if not full_refresh_mode -%}
        {% set final_alias = '' -%}
        {% if alias -%}
            {% set final_alias = alias + '.' -%}
        {% endif -%}

        ,{{ final_alias }}METADATA$ACTION
        ,{{ final_alias }}METADATA$ISUPDATE
        ,{{ final_alias }}METADATA$ROW_ID
    {% endif -%}
{%- endmacro %}

{% macro sfc_get_stream_metadata_filters(alias) -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if not full_refresh_mode -%}
        {% set final_alias = '' -%}
        {% if alias -%}
            {% set final_alias = alias + '.' -%}
        {% endif -%}

        AND NOT ({{ final_alias }}METADATA$ACTION = 'DELETE' AND {{ final_alias }}METADATA$ISUPDATE = 'TRUE')
    {% endif -%}
{%- endmacro %}

{% macro sfc_create_temp_get_alter_sql(target_relation, tmp_relation, sql) -%}
    {#-- Load the model into a real table with temporary name. Need to run this first so the object exists to query below. --#}
    {% do run_query(create_table_as(False, tmp_relation, sql)) %}

    {#-- Drop the Snowflake STREAM metadata columns --#}
    {% set dest_columns = adapter.get_columns_in_relation(tmp_relation) %}
    {% for column in dest_columns -%}
        {% if (column.name == 'METADATA$ACTION') or (column.name == 'METADATA$ISUPDATE') or (column.name == 'METADATA$ROW_ID') %}
            ALTER TABLE {{ tmp_relation }} DROP COLUMN {{ adapter.quote(column.name) }};
        {% endif %}
    {% endfor -%}

    {#-- Rename the table to the final target name --#}
    ALTER TABLE {{ tmp_relation }} RENAME TO {{ target_relation }};
{%- endmacro %}

{% macro sfc_get_stream_merge_sql(target, source, unique_key, dest_columns) -%}
    {% set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}

    MERGE INTO {{ target }} T
    USING {{ source }} S

    {% if unique_key -%}
        ON (T.{{ unique_key }} = S.{{ unique_key }})
    {% else -%}
        ON FALSE
    {% endif -%}

    {% if unique_key -%}
    WHEN MATCHED AND S.METADATA$ACTION = 'DELETE' AND S.METADATA$ISUPDATE = 'FALSE' THEN
        DELETE
    WHEN MATCHED AND S.METADATA$ACTION = 'INSERT' AND S.METADATA$ISUPDATE = 'TRUE' THEN
        UPDATE SET
        {% for column in dest_columns -%}
            T.{{ adapter.quote(column.name) }} = S.{{ adapter.quote(column.name) }}
            {% if not loop.last -%}, {% endif -%}
        {% endfor -%}
    {% endif -%}

    WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' AND S.METADATA$ISUPDATE = 'FALSE' THEN
        INSERT
        ({{ dest_cols_csv }})
        VALUES
        ({{ dest_cols_csv }})

{%- endmacro %}

{% materialization sfc_incremental, adapter='snowflake' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = sfc_create_temp_get_alter_sql(target_relation, tmp_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = sfc_create_temp_get_alter_sql(target_relation, tmp_relation, sql) %}
  {% elif full_refresh_mode %}
    {% set build_sql = sfc_create_temp_get_alter_sql(target_relation, tmp_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Don't include the Snowflake Stream metadata columns --#}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation)
                | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {% set build_sql = sfc_get_stream_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
