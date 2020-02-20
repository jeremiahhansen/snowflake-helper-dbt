{% macro sfc_source(schema_name, table_name) -%}
    {%- set materialization_name = config.require('materialized') -%}

    {% if materialization_name == 'sfc_incremental' %}

        {#-- Create the stream if it does not already exist --#}
        {%- set stream_name = sfc_helper.sfc_get_stream_name(table_name) -%}
        {%- set stream_metadata = sfc_helper.sfc_get_stream_metadata(schema_name, stream_name) -%}
        {%- if not stream_metadata -%}
            {%- do sfc_helper.sfc_create_stream_on_table(schema_name, stream_name, table_name) -%}
        {%- endif -%}

        {#-- Decide between the base table or stream --#}
        {%- set incremental_mode = sfc_helper.sfc_get_incremental_mode() -%}
        {% if incremental_mode %}
            {{ source(schema_name, stream_name) }}
        {%- else -%}
            {{ source(schema_name, table_name) }}
        {%- endif -%}

    {%- else -%}

        {{ source(schema_name, table_name) }}

    {%- endif -%}
{%- endmacro %}

{% macro sfc_ref(model_name) -%}
{%- endmacro %}