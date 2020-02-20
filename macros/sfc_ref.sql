{% macro sfc_source(schema_name, table_name) -%}
    {%- set materialization_name = config.require('materialized') -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

    {% if materialization_name == 'sfc_incremental' %}

        {%- set stream_name = snowflake_helper_dbt.sfc_get_stream_name(table_name) -%}

        {#-- Decide between the base table or stream --#}
        {% if full_refresh_mode %}
            {{ source(schema_name, table_name) }}
        {%- else -%}
            {{ source(schema_name, stream_name) }}
        {%- endif -%}

    {%- else -%}

        {{ source(schema_name, table_name) }}

    {%- endif -%}
{%- endmacro %}

{% macro sfc_ref(model_name) -%}
{%- endmacro %}