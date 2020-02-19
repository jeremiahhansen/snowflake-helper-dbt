{% docs __overview__ %}

# snowflake-helper-dbt
A re-usable Snowflake helper package for dbt

## Macros
### Materializations
Custom materializations which leverage native Snowflake capabilities

#### sfc_incremental
An incremental materialization pattern leveraging Snowflake streams

#### sfc_task
An experimental materialization to manage Snowflake tasks. Not sure if this fits the dbt paradigm correctly.

{% enddocs %}