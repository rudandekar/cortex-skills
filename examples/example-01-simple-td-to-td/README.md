# Example 01: Simple TD-to-TD Mapping

## Overview

This example demonstrates a simple Teradata-to-Teradata mapping with:
- Source Qualifier (with SQL override)
- Pre-session SQL (TRUNCATE)
- Post-session SQL (UPDATE statistics)
- Direct column mapping

## Source Files

- `input/Sample.xml` - Informatica PowerMart export

## Expected Output

### Model: `mart_temp_2526_perm`

```sql
{{ config(
    materialized='table',
    schema='ETL_WRK_DB',
    tags=['wf_m_wk_temp_2526_perm', 'TODO_freq', 'TODO_domain'],
    pre_hook=[
        "DELETE ETL_WRK_DB.Temp_2526_Perm ALL;"
    ],
    post_hook=[
        "COLLECT STATISTICS ETL_WRK_DB.Temp_2526_Perm COLUMN(Emp_Id);"
    ],
    meta={
        'source_workflow': 'm_WK_Temp_2526_Perm',
        'target_table': 'Temp_2526_Perm',
        'generated_by': 'INFA2DBT_accelerator_v1.1'
    }
) }}

WITH source_temp_1234_perm AS (
    -- SQL Override from Source Qualifier
    SELECT
        Emp_Id,
        Emp_Name,
        Emp_Sal
    FROM ETL_WRK_DB.Temp_1234_Perm
    WHERE Emp_Id > 0
),

final AS (
    SELECT
        Emp_Id,
        Emp_Name,
        Emp_Sal
    FROM source_temp_1234_perm
)

SELECT * FROM final
```

### schema.yml

```yaml
version: 2

models:
  - name: mart_temp_2526_perm
    description: >
      Simple TD-to-TD mapping converted from Informatica workflow m_WK_Temp_2526_Perm.
      Direct column passthrough with pre/post SQL hooks.
    meta:
      source_workflow: m_WK_Temp_2526_Perm
      target_table: Temp_2526_Perm
    columns:
      - name: Emp_Id
        description: "Source: Temp_1234_Perm.Emp_Id via Source Qualifier."
        tests:
          - not_null
      - name: Emp_Name
        description: "Source: Temp_1234_Perm.Emp_Name via Source Qualifier."
      - name: Emp_Sal
        description: "Source: Temp_1234_Perm.Emp_Sal via Source Qualifier."
```

## Transformation Chain

1. **SQ_Temp_1234_Perm** (Source Qualifier)
   - Type: Source Qualifier
   - SQL Override: Yes (SELECT with WHERE clause)
   - Input: Temp_1234_Perm
   - Output: 3 columns

2. **Temp_2526_Perm** (Target)
   - Pre-SQL: DELETE ALL
   - Post-SQL: COLLECT STATISTICS

## Notes

- Pre/Post SQL converted to DBT hooks
- Teradata DELETE ALL → Snowflake TRUNCATE equivalent
- COLLECT STATISTICS → Consider removing for Snowflake (automatic)
