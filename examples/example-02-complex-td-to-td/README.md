# Example 02: Complex TD-to-TD with Multiple Transformations

## Overview

This example demonstrates a complex mapping with:
- Multiple Source Qualifiers (with SQL overrides)
- Lookups (connected)
- Aggregator
- Router (multiple output groups)
- Update Strategy (incremental)
- Filter
- Joiner

## Source Files

- `input/Sample2.xml` - Informatica PowerMart export

## Expected Output

### Decomposition

This workflow produces **3 target tables**, therefore 3 DBT models:
1. `mart_temp_2527_perm` - Main output
2. `mart_temp_2528_perm` - Router Group 1
3. `mart_temp_2529_perm` - Router Group 2 (default)

### Model 1: `mart_temp_2527_perm`

```sql
{{ config(
    materialized='incremental',
    unique_key='Emp_Id',
    schema='ETL_WRK_DB',
    tags=['wf_m_wk_temp_2527_perm', 'TODO_freq', 'TODO_domain'],
    meta={
        'source_workflow': 'm_WK_Temp_2527_Perm',
        'target_table': 'Temp_2527_Perm',
        'generated_by': 'INFA2DBT_accelerator_v1.1'
    }
) }}

WITH source_temp_1234_perm AS (
    SELECT
        Emp_Id,
        Emp_Name,
        Emp_Sal,
        Dept_Id
    FROM {{ source('ETL_WRK_DB', 'Temp_1234_Perm') }}
    WHERE Emp_Id > 0
),

source_dept_lookup AS (
    SELECT
        Dept_Id,
        Dept_Name,
        Manager_Id
    FROM {{ source('ETL_WRK_DB', 'Dept_Master') }}
),

joined_with_dept AS (
    SELECT
        e.Emp_Id,
        e.Emp_Name,
        e.Emp_Sal,
        d.Dept_Name,
        d.Manager_Id
    FROM source_temp_1234_perm e
    LEFT JOIN source_dept_lookup d
        ON e.Dept_Id = d.Dept_Id
),

aggregated_by_dept AS (
    SELECT
        Dept_Name,
        SUM(Emp_Sal) AS Total_Salary,
        COUNT(Emp_Id) AS Emp_Count,
        MAX(Emp_Sal) AS Max_Salary
    FROM joined_with_dept
    GROUP BY Dept_Name
),

filtered_active AS (
    SELECT *
    FROM aggregated_by_dept
    WHERE Total_Salary > 10000
),

final AS (
    SELECT
        Dept_Name,
        Total_Salary,
        Emp_Count,
        Max_Salary
    FROM filtered_active
)

SELECT * FROM final
{% if is_incremental() %}
WHERE _updated_at > (SELECT MAX(_updated_at) FROM {{ this }})
{% endif %}
```

### Model 2: `mart_temp_2528_perm` (Router Group: HIGH_SALARY)

```sql
{{ config(
    materialized='table',
    schema='ETL_WRK_DB',
    tags=['wf_m_wk_temp_2527_perm', 'router_high_salary', 'TODO_domain'],
    meta={
        'source_workflow': 'm_WK_Temp_2527_Perm',
        'target_table': 'Temp_2528_Perm',
        'router_group': 'HIGH_SALARY',
        'generated_by': 'INFA2DBT_accelerator_v1.1'
    }
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('int_pre_router_data') }}
),

router_high_salary AS (
    SELECT *
    FROM source_data
    WHERE Emp_Sal > 100000
),

final AS (
    SELECT * FROM router_high_salary
)

SELECT * FROM final
```

## Transformation Chain

```
┌─────────────────────┐
│ SQ_Temp_1234_Perm   │
│ (Source Qualifier)  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐     ┌─────────────────────┐
│ LKP_Dept_Master     │◄────│ Dept_Master         │
│ (Lookup - Connected)│     │ (Lookup Source)     │
└─────────┬───────────┘     └─────────────────────┘
          │
          ▼
┌─────────────────────┐
│ AGG_By_Dept         │
│ (Aggregator)        │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ FIL_Active          │
│ (Filter)            │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ RTR_Salary_Tier     │
│ (Router)            │
├─────────┬───────────┤
│ HIGH    │ DEFAULT   │
└────┬────┴─────┬─────┘
     │          │
     ▼          ▼
┌─────────┐ ┌─────────┐
│Target 1 │ │Target 2 │
└─────────┘ └─────────┘
```

## Transformation Types Used

| Type | Instance | Notes |
|------|----------|-------|
| Source Qualifier | SQ_Temp_1234_Perm | SQL override present |
| Lookup (Connected) | LKP_Dept_Master | LEFT JOIN pattern |
| Aggregator | AGG_By_Dept | SUM, COUNT, MAX |
| Filter | FIL_Active | WHERE clause |
| Router | RTR_Salary_Tier | 2 output groups |
| Update Strategy | UPD_Strategy | Incremental materialization |

## Notes

- Router produces 2 separate models (one per output group)
- Update Strategy converted to DBT incremental materialization
- Lookup converted to LEFT JOIN (connected lookup pattern)
