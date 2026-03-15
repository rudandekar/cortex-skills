# Informatica to DBT Transformation Type Map

This reference document maps all supported Informatica PowerCenter transformation types to their DBT/Snowflake SQL equivalents.

## Core Transformation Types

### 1. Source Qualifier

**Informatica Purpose:** Reads data from relational sources, can include SQL override.

**DBT Pattern:**

```sql
-- Without SQL override
WITH source_{table} AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
)

-- With SQL override
WITH source_{table} AS (
    -- Informatica SQL override content
    {sql_override_from_TRANSFORMATION.TABLEATTRIBUTE[@NAME='Sql Query']}
)
```

**Field Mapping:**
| Informatica | DBT |
|-------------|-----|
| `SOURCEFIELD.NAME` | Column name |
| `SOURCEFIELD.DATATYPE` | Snowflake data type |
| SQL Query attribute | CTE body |

---

### 2. Expression

**Informatica Purpose:** Row-level calculations and transformations.

**DBT Pattern:**

```sql
SELECT
    {passthrough_columns},
    {expression_logic} AS {output_port_name}
FROM {previous_cte}
```

**Expression Translation Rules:**

| Informatica Function | Snowflake Equivalent |
|---------------------|---------------------|
| `IIF(cond, true, false)` | `IFF(cond, true, false)` |
| `DECODE(val, match1, result1, ...)` | `CASE val WHEN match1 THEN result1 ... END` |
| `LTRIM(str)` | `LTRIM(str)` |
| `RTRIM(str)` | `RTRIM(str)` |
| `TO_CHAR(date, 'fmt')` | `TO_CHAR(date, 'fmt')` |
| `TO_DATE(str, 'fmt')` | `TO_DATE(str, 'fmt')` |
| `ADD_TO_DATE(date, 'interval', n)` | `DATEADD(interval, n, date)` |
| `TRUNC(date, 'interval')` | `DATE_TRUNC(interval, date)` |
| `SYSDATE` | `CURRENT_TIMESTAMP()` |
| `NVL(val, default)` | `NVL(val, default)` or `COALESCE(val, default)` |
| `NVL2(val, not_null_result, null_result)` | `IFF(val IS NOT NULL, not_null_result, null_result)` |
| `INSTR(str, substr)` | `CHARINDEX(substr, str)` |
| `SUBSTR(str, start, len)` | `SUBSTR(str, start, len)` |
| `LPAD(str, len, pad)` | `LPAD(str, len, pad)` |
| `RPAD(str, len, pad)` | `RPAD(str, len, pad)` |
| `CONCAT(a, b)` | `CONCAT(a, b)` or `a \|\| b` |
| `REG_EXTRACT(str, pattern)` | `REGEXP_SUBSTR(str, pattern)` |
| `REG_REPLACE(str, pattern, repl)` | `REGEXP_REPLACE(str, pattern, repl)` |
| `ERROR('message')` | `-- ERROR: handle in dbt test` |
| `ABORT('message')` | `-- ABORT: handle in dbt test` |
| `LOOKUP(...)` | See Lookup transformation |

---

### 3. Aggregator

**Informatica Purpose:** Group-level calculations (SUM, COUNT, AVG, etc.).

**DBT Pattern:**

```sql
SELECT
    {group_by_columns},
    SUM({column}) AS {sum_port},
    COUNT({column}) AS {count_port},
    AVG({column}) AS {avg_port},
    MAX({column}) AS {max_port},
    MIN({column}) AS {min_port},
    COUNT(DISTINCT {column}) AS {distinct_count_port}
FROM {previous_cte}
GROUP BY {group_by_columns}
```

**Field Mapping:**
| Informatica Port Type | DBT |
|----------------------|-----|
| Group By port | GROUP BY column |
| Aggregate port with `SUM(...)` | SUM aggregate |
| Aggregate port with `COUNT(...)` | COUNT aggregate |
| Aggregate port with `FIRST(...)` | `FIRST_VALUE(...) OVER (...)` |
| Aggregate port with `LAST(...)` | `LAST_VALUE(...) OVER (...)` |
| Variable port | Intermediate calculation (include in SELECT) |

---

### 4. Lookup (Connected)

**Informatica Purpose:** Retrieves values from lookup table based on join condition.

**DBT Pattern:**

```sql
SELECT
    a.*,
    b.{return_port_1},
    b.{return_port_2}
FROM {previous_cte} a
LEFT JOIN {{ source('{lookup_schema}', '{lookup_table}') }} b
    ON a.{lookup_key_1} = b.{lookup_key_1}
    AND a.{lookup_key_2} = b.{lookup_key_2}
```

**Lookup Condition Extraction:**
- Parse `TRANSFORMATION.TABLEATTRIBUTE[@NAME='Lookup condition']`
- Convert to JOIN ON clause

**Return All Rows vs First Row:**
- If `Return All Rows = Yes`: Use JOIN (may produce multiple rows)
- If `Return All Rows = No`: Use subquery with LIMIT 1 or ROW_NUMBER

---

### 5. Lookup (Unconnected)

**Informatica Purpose:** Called from Expression transformation via :LKP function.

**DBT Pattern:**

```sql
SELECT
    *,
    (
        SELECT {return_column}
        FROM {{ source('{lookup_schema}', '{lookup_table}') }}
        WHERE {lookup_condition}
        LIMIT 1
    ) AS {output_column}
FROM {previous_cte}
```

**Note:** Correlated subquery may impact performance. Consider refactoring to connected lookup in optimization phase.

---

### 6. Joiner

**Informatica Purpose:** Joins two data flows.

**DBT Pattern:**

```sql
SELECT
    {selected_columns}
FROM {master_cte} m
{join_type} JOIN {detail_cte} d
    ON m.{join_key_1} = d.{join_key_1}
    AND m.{join_key_2} = d.{join_key_2}
```

**Join Type Mapping:**
| Informatica | SQL |
|-------------|-----|
| Normal | INNER JOIN |
| Master Outer | LEFT JOIN |
| Detail Outer | RIGHT JOIN |
| Full Outer | FULL OUTER JOIN |

**Master vs Detail:**
- `MASTER` source → left side of join
- `DETAIL` source → right side of join

---

### 7. Filter

**Informatica Purpose:** Filters rows based on condition.

**DBT Pattern:**

```sql
SELECT *
FROM {previous_cte}
WHERE {filter_condition}
```

**Condition Translation:**
- Parse `TRANSFORMATION.TABLEATTRIBUTE[@NAME='Filter Condition']`
- Apply expression translation rules from Expression section

---

### 8. Router

**Informatica Purpose:** Routes rows to different output groups based on conditions.

**DBT Pattern (separate models per group):**

```sql
-- Model for Group 1: {group_name_1}
SELECT *
FROM {previous_cte}
WHERE {group_1_condition}

-- Model for Group 2: {group_name_2}
SELECT *
FROM {previous_cte}
WHERE {group_2_condition}

-- Model for DEFAULT group (no condition match)
SELECT *
FROM {previous_cte}
WHERE NOT ({group_1_condition})
  AND NOT ({group_2_condition})
```

**Important:** Each Router output group typically maps to a separate DBT model or a separate CTE that feeds different downstream targets.

---

### 9. Normalizer

**Informatica Purpose:** Converts columns to rows (unpivot).

**DBT Pattern (Snowflake LATERAL FLATTEN):**

```sql
SELECT
    {base_columns},
    f.seq AS {gcid_column},
    f.index + 1 AS {gck_column},
    f.value:{field_1}::STRING AS {normalized_field_1},
    f.value:{field_2}::NUMBER AS {normalized_field_2}
FROM {previous_cte},
LATERAL FLATTEN(input => ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('field_1', {col_1}, 'field_2', {col_2}),
    OBJECT_CONSTRUCT('field_1', {col_3}, 'field_2', {col_4})
)) f
```

**Alternative (UNPIVOT):**

```sql
SELECT
    {base_columns},
    attribute_name,
    attribute_value
FROM {previous_cte}
UNPIVOT (
    attribute_value FOR attribute_name IN ({col_1}, {col_2}, {col_3})
)
```

---

### 10. Rank

**Informatica Purpose:** Ranks rows within groups.

**DBT Pattern:**

```sql
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY {group_by_columns}
        ORDER BY {rank_columns} {ASC|DESC}
    ) AS {rank_port}
FROM {previous_cte}
-- Optional: filter to top N
QUALIFY {rank_port} <= {top_n}
```

**Rank Function Mapping:**
| Informatica | Snowflake |
|-------------|-----------|
| RANK index (dense) | DENSE_RANK() |
| RANK index (skip) | RANK() |
| Top/Bottom N | QUALIFY with ROW_NUMBER() |

---

### 11. Update Strategy

**Informatica Purpose:** Determines insert/update/delete/reject for each row.

**DBT Pattern (Incremental):**

```sql
{{ config(
    materialized='incremental',
    unique_key='{primary_key_columns}',
    merge_update_columns=['{update_col_1}', '{update_col_2}'],
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
    {% if is_incremental() %}
    WHERE {updated_timestamp_col} > (SELECT MAX({updated_timestamp_col}) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    -- Update strategy flag translation
    CASE
        WHEN {insert_condition} THEN 'INSERT'
        WHEN {update_condition} THEN 'UPDATE'
        WHEN {delete_condition} THEN 'DELETE'
        ELSE 'REJECT'
    END AS _dbt_update_strategy
FROM source
WHERE NOT ({reject_condition})  -- Filter out rejects
```

**Update Strategy Values:**
| Informatica | Value | DBT Handling |
|-------------|-------|--------------|
| DD_INSERT | 0 | Included in incremental |
| DD_UPDATE | 1 | Merged via unique_key |
| DD_DELETE | 2 | Not directly supported — use soft deletes or snapshots |
| DD_REJECT | 3 | Filtered out with WHERE |

---

### 12. Sequence Generator

**Informatica Purpose:** Generates sequential numbers.

**DBT Pattern:**

```sql
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY {deterministic_order_column}) + {start_value} - 1 AS {sequence_port}
FROM {previous_cte}
```

**For CURRVAL/NEXTVAL semantics:**

```sql
-- Create Snowflake sequence if persistent sequence needed
CREATE SEQUENCE IF NOT EXISTS {schema}.{sequence_name}
    START = {start_value}
    INCREMENT = {increment_by};

-- Use in model
SELECT
    *,
    {schema}.{sequence_name}.NEXTVAL AS {sequence_port}
FROM {previous_cte}
```

**Note:** Snowflake sequences are not transaction-safe in the same way as Informatica. Consider using ROW_NUMBER() for deterministic results.

---

## Transformation Type Coverage Status

| Type | Coverage | Notes |
|------|----------|-------|
| Source Qualifier | ✅ Full | Including SQL override |
| Expression | ✅ Full | Function translation may need extension |
| Aggregator | ✅ Full | All aggregate functions |
| Lookup (Connected) | ✅ Full | JOIN pattern |
| Lookup (Unconnected) | ✅ Full | Correlated subquery |
| Joiner | ✅ Full | All join types |
| Filter | ✅ Full | Direct translation |
| Router | ✅ Full | Separate models per group |
| Normalizer | ✅ Full | LATERAL FLATTEN |
| Rank | ✅ Full | Window functions |
| Update Strategy | ✅ Full | Incremental materialization |
| Sequence Generator | ✅ Full | ROW_NUMBER or sequence |
| Stored Procedure | ⚠️ Escalate | Requires manual review |
| Custom Transformation | ⚠️ Escalate | Requires manual review |
| Java Transformation | ⚠️ Escalate | Requires Snowpark UDF |
| XML Parser | ⚠️ Partial | XMLGET functions |
| HTTP Transformation | ⚠️ Escalate | External function |

---

## Pre/Post SQL Handling

**Pre-Session SQL:**
- Extract from `SESSION.SESSIONEXTENSION[@NAME='Pre-Session SQL']`
- Execute as Snowflake stored procedure or DBT hook

**Post-Session SQL:**
- Extract from `SESSION.SESSIONEXTENSION[@NAME='Post-Session SQL']`
- Execute as Snowflake stored procedure or DBT hook

**DBT Hook Pattern:**

```sql
{{ config(
    pre_hook=[
        "TRUNCATE TABLE {{ this }};",
        "CALL prepare_staging();"
    ],
    post_hook=[
        "CALL update_audit_log('{{ this }}', '{{ run_started_at }}');"
    ]
) }}
```
