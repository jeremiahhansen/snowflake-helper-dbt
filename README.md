# snowflake-helper-dbt
A re-usable Snowflake helper package for dbt

## Macros

### General
General macros to help working with Snowflake

#### sfc_source(schema_name, table_name) ([source](macros/sfc_ref.sql))
This macro is a wrapper around the builtin source() function which allows for dynamicly changing which source table is referenced by the current model.

Usage:
```
FROM {{ snowflake_helper_dbt.sfc_source('CITIBIKE', 'PROGRAMS') }}
```

Parameters:
* `schema_name`: the name of the schema where the source table resides
* `table_name`: the name of the source table

#### sfc_ref(model_name) ([source](macros/sfc_ref.sql))
This macro is a wrapper around the builtin ref() function which allows for dynamicly changing which model is referenced by the current model.


### Materializations
Custom materializations which leverage native Snowflake capabilities

#### sfc_incremental ([source](macros/materializations/sfc_incremental.sql))
An incremental materialization pattern which leverages Snowflake streams for CDC.

This materialization is appropriate for cases when the source table does not have an update timestamp column or when performance is poor. If the stream does not exist on the table then the materialization will create it.

Usage:
```sql
{{
    config(
        materialized='sfc_incremental',
        pre_hook="{{ snowflake_helper_dbt.sfc_get_create_stream_ddl('CITIBIKE','PROGRAMS') }}",
        transient=false,
        unique_key='PROGRAM_ID' 
    )
}}

SELECT
     PROGRAM_ID
    ,PROGRAM_NAME
    ,RANGE_START
    ,RANGE_END
    {{ snowflake_helper_dbt.sfc_get_stream_metadata_columns() }}
FROM {{ snowflake_helper_dbt.sfc_source('CITIBIKE', 'PROGRAMS') }}
WHERE 1 = 1
    {{ snowflake_helper_dbt.sfc_get_stream_metadata_filters() }}
```

Configuration values:
* `pre_hook`: use the `snowflake_helper_dbt.sfc_get_create_stream_ddl()` macro to generate the SQL needed to create the stream if it doesn't exist
* `unique_key`: the column used to uniquely identify a record and which is used to determine if a record has changed


#### sfc_task ([source](macros/materializations/sfc_task.sql))

An experimental materialization to manage Snowflake tasks. Not sure if this fits the dbt paradigm correctly.
