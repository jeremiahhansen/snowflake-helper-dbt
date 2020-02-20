# snowflake-helper-dbt
A re-usable Snowflake helper package for dbt

## Macros
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
        pre_hook="{{ sfc_get_create_stream_ddl('CITIBIKE','PROGRAMS') }}",
        transient=false,
        unique_key='PROGRAM_ID' 
    )
}}

SELECT
     PROGRAM_ID
    ,PROGRAM_NAME
    ,RANGE_START
    ,RANGE_END
    {{ sf_get_stream_metadata_columns() }}
FROM {{ sfc_source('CITIBIKE', 'PROGRAMS') }}
WHERE 1 = 1
    {{ sf_get_stream_metadata_filters() }}
```

Configuration values:
* `unique_key`: the column used to uniquely identify a record and which is used to determine if a record has changed


#### sfc_task ([source](macros/materializations/sfc_task.sql))

An experimental materialization to manage Snowflake tasks. Not sure if this fits the dbt paradigm correctly.
