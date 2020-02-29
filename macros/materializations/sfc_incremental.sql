{% macro sfc_get_incremental_mode() -%}
    {% set incremental_mode = False %}
    {% set existing_relation = load_relation(this) %}
    {% set full_refresh_mode = (flags.FULL_REFRESH == True) %}

    {%- if existing_relation is not none and existing_relation.is_table and not full_refresh_mode -%}
        {% set incremental_mode = True %}
    {% endif -%}

    {{ return(incremental_mode) }}
{%- endmacro %}

{% macro sfc_get_stream_name(table_name) -%}
    {{ return(table_name + '_STREAM') }}
{%- endmacro %}

{% macro sfc_get_stream_metadata(schema_name, stream_name) -%}
    {%- set metadata = False -%}

    {#-- TODO: Need to add database or schema filtering --#}
    {% call statement('show_stream', fetch_result=True) -%}
        SHOW STREAMS LIKE '{{ stream_name }}'
    {%- endcall %}

    {%- set show_result = load_result('show_stream') -%}
    {% set metadata = show_result['data'] %}
    {% if metadata|count == 0 %}
        {% set metadata = False %}
    {% endif %}

    {{ return(metadata) }}
{%- endmacro %}

{% macro sfc_create_stream_on_table(schema_name, stream_name, table_name) -%}
    {%- set stream_relation = api.Relation.create(schema=schema_name, identifier=stream_name) %}
    {%- set table_relation = api.Relation.create(schema=schema_name, identifier=table_name) %}

    {% call statement('create_stream') -%}
        CREATE STREAM {{ stream_relation }} ON TABLE {{ table_relation }}
    {%- endcall %}

    {{ log("Created stream " ~ stream_relation ~ " on table " ~ table_relation ~ ".") }}
{%- endmacro %}

{% macro sfc_get_stream_metadata_columns(alias) -%}
    {% set incremental_mode = sfc_helper.sfc_get_incremental_mode() %}
    {%- if incremental_mode -%}
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
    {% set incremental_mode = sfc_helper.sfc_get_incremental_mode() %}
    {%- if incremental_mode -%}
        {% set final_alias = '' -%}
        {% if alias -%}
            {% set final_alias = alias + '.' -%}
        {% endif -%}

        AND NOT ({{ final_alias }}METADATA$ACTION = 'DELETE' AND {{ final_alias }}METADATA$ISUPDATE = 'TRUE')
    {% endif -%}
{%- endmacro %}

{% macro sfc_get_stream_merge_sql(target_relation, source_relation, unique_key) -%}
    {#-- Don't include the Snowflake Stream metadata columns --#}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation)
                | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {% set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}

    MERGE INTO {{ target_relation }} T
    USING {{ source_relation }} S

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

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {% set incremental_mode = sfc_helper.sfc_get_incremental_mode() %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- If the target relation already exists as a view drop it now --#}
  {% if existing_relation.is_view %}
    {{ log("Dropping relation " ~ existing_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
  {% endif %}

  {#-- Option #1: We need to incrementally add data to the target --#}
  {% if incremental_mode %}
    {#-- Load data from compiled model sql query into a temp table --#}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {% set build_sql = sfc_helper.sfc_get_stream_merge_sql(target_relation, tmp_relation, unique_key) %}

  {#-- Option #2: We need to create/recreate the target --#}
  {% else %}
    {#-- Load data from compiled model sql query into a real table --#}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
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
