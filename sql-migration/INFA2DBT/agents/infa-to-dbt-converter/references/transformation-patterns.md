# Transformation SQL Patterns

Standard conversion patterns for each Informatica transformation type.

## Source Qualifier (no SQL override)
```sql
WITH source_{table} AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
)
```

## Source Qualifier (with SQL override)
```sql
WITH source_{table} AS (
    -- SQL Override from Informatica
    {sql_override_content}
)
```

## Expression
```sql
SELECT
    {original_columns},
    {expression_logic} AS {output_column}
FROM {previous_cte}
```

## Aggregator
```sql
SELECT
    {group_by_columns},
    SUM({agg_column}) AS {output_sum},
    COUNT({agg_column}) AS {output_count},
    MAX({agg_column}) AS {output_max}
FROM {previous_cte}
GROUP BY {group_by_columns}
```

## Lookup (connected)
```sql
SELECT
    a.*,
    b.{lookup_return_column}
FROM {previous_cte} a
LEFT JOIN {{ source('{lookup_schema}', '{lookup_table}') }} b
    ON a.{lookup_key} = b.{lookup_key}
```

## Lookup (unconnected)
```sql
SELECT
    *,
    (SELECT {return_col} FROM {lookup_table} WHERE {condition} LIMIT 1) AS {output_col}
FROM {previous_cte}
```

## Joiner
```sql
SELECT
    {selected_columns}
FROM {left_cte} a
{join_type} JOIN {right_cte} b
    ON a.{join_key} = b.{join_key}
```

## Filter
```sql
SELECT *
FROM {previous_cte}
WHERE {filter_condition}
```

## Router
```sql
-- Output group: {group_name}
SELECT *
FROM {previous_cte}
WHERE {group_condition}
```

## Normalizer (Snowflake LATERAL FLATTEN)
```sql
SELECT
    {base_columns},
    f.value AS {normalized_column}
FROM {previous_cte},
LATERAL FLATTEN(input => {array_column}) f
```

## Rank
```sql
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {order_cols}) AS rn
FROM {previous_cte}
```

## Update Strategy (Incremental)
```sql
{{ config(
    materialized='incremental',
    unique_key='{primary_key}',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
    {% if is_incremental() %}
    WHERE {updated_at_col} > (SELECT MAX({updated_at_col}) FROM {{ this }})
    {% endif %}
)
```
