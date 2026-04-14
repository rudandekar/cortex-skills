# dbt Conventions Checklist

Reference document for `dbt-model-optimizer` Step 3 (Dimension D2). Defines the
code quality standards for dbt models, particularly post-migration models from
Informatica/legacy SQL.

---

## Table of Contents

1. [Config Block Requirements](#config-block-requirements)
2. [CTE Structure Rules](#cte-structure-rules)
3. [Naming Conventions](#naming-conventions)
4. [Dependency Rules (source/ref/this)](#dependency-rules)
5. [Hook Configuration](#hook-configuration)
6. [Audit Column Rules](#audit-column-rules)
7. [Materialization Guidelines](#materialization-guidelines)

---

## Config Block Requirements

Every dbt model MUST have a `{{ config(...) }}` block at the top of the file.

### Minimum required keys:

```jinja
{{ config(
    materialized='table',           -- REQUIRED: table, view, incremental, ephemeral, dynamic_table
    schema='target_schema',         -- REQUIRED: target schema name
    database=var('target_db'),      -- REQUIRED for cross-database: use var() for portability
    query_tag='wf_workflow_name',   -- RECOMMENDED: enables query history tracking
    tags=['workflow_name', 'daily'] -- RECOMMENDED: enables selection by tag
) }}
```

### Additional keys for incremental models:

```jinja
{{ config(
    materialized='incremental',
    incremental_strategy='append',  -- REQUIRED for incremental: append, delete+insert, merge
    on_schema_change='append_new_columns',  -- REQUIRED for incremental
    unique_key='primary_key_column' -- REQUIRED for merge strategy
) }}
```

### Config block validation rules:

| Check | Rule ID | Required | Auto-fixable |
|-------|---------|----------|--------------|
| Config block exists | D2-CFG-001 | Yes | Yes |
| `materialized` is set | D2-CFG-002 | Yes | Yes (infer from model) |
| `schema` is set | D2-CFG-003 | Yes | Yes (infer from path) |
| `database=var()` for cross-db | D2-CFG-004 | Conditional | Yes |
| `query_tag` present | D2-CFG-005 | Recommended | Yes (use model name) |
| `on_schema_change` for incremental | D2-CFG-006 | Yes (incremental) | Yes |
| `incremental_strategy` for incremental | D2-CFG-007 | Yes (incremental) | Yes |

---

## CTE Structure Rules

### Standard CTE pattern:

```sql
WITH

source_data AS (
    SELECT ...
    FROM {{ source('schema', 'table') }}
    WHERE ...
),

transformed AS (
    SELECT ...
    FROM source_data
    ...
),

final AS (
    SELECT ...
    FROM transformed
)

SELECT * FROM final
```

### CTE rules:

1. **Every model should use CTEs** — no deeply nested subqueries
2. **Last CTE should be named `final`** — convention for clarity
3. **Final SELECT should be `SELECT * FROM final`** — keeps the output declaration in the CTE
4. **No unused CTEs** — every defined CTE must be referenced downstream (D2-CTE-001)
5. **No redundant CTEs** — two CTEs with identical SQL should be consolidated (D2-CTE-002)
6. **No inline subqueries in final SELECT** — extract to named CTEs (D2-CTE-003)
7. **CTE names are descriptive** — `source_orders` not `cte1`

---

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Model file name | `snake_case.sql` | `n_iam_sales_terr_lnk_tv.sql` |
| CTE names | `snake_case`, descriptive | `source_accounts`, `filtered_orders` |
| Column aliases | `snake_case` | `customer_id`, `order_date` |
| Config schema | `snake_case` | `'mart_sales'` |
| Tags | `snake_case` | `['wf_daily_sales', 'daily']` |

### Model file name prefixes (Cisco convention):

| Prefix | Layer | Typical Materialization |
|--------|-------|------------------------|
| `N_` | Normalized/staging | table (source-loaded) |
| `MT_` | Master table | table or incremental |
| `WI_` | Work intermediate | table |
| `W_` | Work | table or view |
| `ST_` | Staging | table |
| `R_` | Reference | table (source-loaded) |
| `EX_` | Extract | table |
| `EL_` | Element | table or view |
| `SM_` | Summary | table |
| `GL_` | Global | table |
| `FA_` | Fact | table |

---

## Dependency Rules

### source() vs ref() vs {{ this }}

| Table Prefix | External? | Correct Macro | Example |
|-------------|-----------|---------------|---------|
| N_, MT_, R_ | Yes (loaded by external process) | `{{ source('schema', 'table') }}` | `{{ source('raw_sales', 'N_ORDERS') }}` |
| W_, WI_, ST_, EX_, EL_, SM_, GL_, FA_ | No (dbt-managed) | `{{ ref('model_name') }}` | `{{ ref('w_order_summary') }}` |
| Self-reference in incremental | — | `{{ this }}` | `FROM {{ this }} WHERE ...` |

### Critical rules:

1. **NEVER use raw table references** like `SCHEMA.TABLE` — always use `source()` or `ref()` (D2-DEP-001)
2. **W_/WI_/ST_/EX_/EL_/SM_/GL_/FA_ tables ALWAYS use ref()** — never penalize ref() for these prefixes (D2-DEP-002, Guardrail NOTE 1)
3. **N_/MT_/R_ external tables use source()** — these are loaded outside dbt (D2-DEP-003)
4. **Incremental models use {{ this }}** for self-references in the SQL body (D2-DEP-004)
5. **Cross-mapping dependencies use ref()** — when one dbt model depends on another

---

## Hook Configuration

### When hooks are required:

If the extracted Informatica JSON or comments indicate pre-SQL or post-SQL exists,
hooks MUST be configured in the dbt model.

### Hook syntax:

```jinja
{{ config(
    materialized='incremental',
    pre_hook=[
        "ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'",
        "{{ edw_convert_to_pst() }}"
    ],
    post_hook=[
        "UPDATE {{ this }} SET EDWSF_UPDATE_DTM = {{ edw_convert_to_pst() }} WHERE EDWSF_UPDATE_DTM IS NULL"
    ]
) }}
```

### Hook rules:

1. **Multi-statement SQL → separate array entries** — each SQL statement is a separate string in the pre_hook/post_hook array
2. **Session-level SQL MUST be captured** — `ALTER SESSION`, `SET` commands → pre_hook (D2-HOOK-003)
3. **Informatica variables replaced** — `$$STGDB` → actual schema or `var()` reference
4. **Correct macro name**: `{{ edw_convert_to_pst() }}` — NOT `{{ edw_pst_timestamp() }}` or similar (D2-HOOK-004)
5. **edw_convert_to_pst() is ONLY valid in hooks** — NEVER in SELECT column expressions (D2-AUDIT-005)

---

## Audit Column Rules

### Naming convention:

| Old (WRONG) | New (CORRECT) | Notes |
|-------------|---------------|-------|
| `EDW_CREATE_DTM` | `EDWSF_CREATE_DTM` | All target models |
| `EDW_UPDATE_DTM` | `EDWSF_UPDATE_DTM` | All target models |
| `EDW_CREATE_USER` | `EDWSF_CREATE_USER` | All target models |
| `EDW_UPDATE_USER` | `EDWSF_UPDATE_USER` | All target models |
| `EDW_BATCH_ID` | `EDWSF_BATCH_ID` | N_ and MT_ models only |

### Audit column expressions by model prefix:

#### MT_ (Master Table) models — SELECT column expressions:

```sql
-- DateTime columns: CURRENT_TIMESTAMP(0)
CURRENT_TIMESTAMP(0) AS EDWSF_CREATE_DTM,
CURRENT_TIMESTAMP(0) AS EDWSF_UPDATE_DTM,

-- User columns: CAST(CURRENT_USER() AS VARCHAR(30))
CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_CREATE_USER,
CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_UPDATE_USER,

-- Batch ID (required for N_ and MT_):
{{ var('batch_id', 0) }} AS EDWSF_BATCH_ID
```

#### N_ (Normalized) models — SELECT column expressions:

Same as MT_ for audit columns. `EDWSF_BATCH_ID` is REQUIRED.

#### WI_ (Work Intermediate) models:

Use `EDWSF_*` naming. `EDWSF_BATCH_ID` is NOT required (but acceptable).

### Critical guardrails:

1. **NEVER use edw_convert_to_pst() in SELECT** — it is ONLY for hook SQL strings (D2-AUDIT-005)
2. **ALWAYS use CURRENT_TIMESTAMP(0) for DTM columns** in SELECT expressions — NEVER penalize this
3. **ALWAYS use CAST(CURRENT_USER() AS VARCHAR(30)) for USER columns** — penalize if CURRENT_TIMESTAMP(0) appears for a USER column
4. **EDWSF_BATCH_ID required for N_ and MT_ prefixes** — not for other prefixes

---

## Materialization Guidelines

| Materialization | When to Use | Key Config |
|----------------|-------------|------------|
| `table` | Default for most models; rebuilt on each run | `materialized='table'` |
| `view` | Lightweight transforms, rarely queried, always need fresh data | `materialized='view'` |
| `incremental` | Large tables with append/upsert pattern; hooks present | `materialized='incremental'`, `incremental_strategy`, `on_schema_change` |
| `ephemeral` | Small helper CTEs referenced by one model | `materialized='ephemeral'` |
| `dynamic_table` | Pure transforms with no hooks; Snowflake-native scheduling | `materialized='dynamic_table'`, `target_lag` |

### When to choose incremental:

- Model has `pre_hook` or `post_hook` → likely incremental
- Model uses `{{ this }}` for self-reference → incremental
- Model processes append-only data (new rows only) → incremental with `append` strategy
- Model does upsert/merge → incremental with `merge` strategy

### When NOT to use incremental:

- Model is a simple SELECT transform with no state
- Model rebuilds completely each time (full refresh pattern)
- Model has complex logic that doesn't benefit from incremental processing
