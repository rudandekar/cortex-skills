# INFA2DBT Coding Guidelines Template

**Version:** 2.3  
**Last Updated:** 2026-03-15  
**Status:** Template — customize for each engagement

---

## 1. Overview

This document defines the coding standards for DBT models converted from Informatica PowerCenter workflows using the INFA2DBT accelerator. All generated models must comply with these guidelines.

**Customization Note:** Sections marked with `[CUSTOMIZE]` require engagement-specific values.

---

## 2. Naming Conventions

### 2.1 Model Names

| Prefix | Purpose | Materialization |
|--------|---------|-----------------|
| `stg_` | Staging — raw data with minimal transformation | `view` |
| `int_` | Intermediate — business logic, joins, aggregations | `ephemeral` or `view` |
| `mart_` | Mart — final consumption layer | `table` |
| `snap_` | Snapshot — SCD Type 2 tracking | `snapshot` |

**Pattern:** `{prefix}_{domain}_{entity}_{qualifier}`

**Examples:**
- `stg_erp_sales_orders`
- `int_crm_customer_enriched`
- `mart_finance_revenue_daily`

### 2.2 Column Names

- Use `snake_case` exclusively
- No spaces, hyphens, or special characters
- Avoid SQL reserved words (`date`, `time`, `user`) — use suffixes like `_date`, `_time`, `_user_id`
- Boolean columns: prefix with `is_`, `has_`, `can_`
- Timestamp columns: suffix with `_at` (e.g., `created_at`, `updated_at`)
- Date columns: suffix with `_date` (e.g., `order_date`)
- ID columns: suffix with `_id` (e.g., `customer_id`)

### 2.3 CTE Names

| CTE Type | Pattern | Example |
|----------|---------|---------|
| Source | `source_{table}` | `source_customers` |
| Filtered | `filtered_{description}` | `filtered_active_customers` |
| Joined | `joined_{entities}` | `joined_orders_customers` |
| Aggregated | `aggregated_{metric}` | `aggregated_sales_by_region` |
| Final | `final` | `final` |

---

## 3. File Organization

### 3.1 Directory Structure

```
models/
├── staging/
│   └── {source_system}/
│       ├── stg_{source}_{table}.sql
│       └── _stg_{source}__models.yml
├── intermediate/
│   └── {domain}/
│       ├── int_{domain}_{entity}.sql
│       └── _int_{domain}__models.yml
├── marts/
│   └── {domain}/
│       ├── mart_{domain}_{entity}.sql
│       └── _mart_{domain}__models.yml
└── converted/              # INFA2DBT output location
    ├── {model}.sql
    └── {model}.schema.yml
```

### 3.2 [CUSTOMIZE] Domain Tags

Define your domain taxonomy:

```yaml
domains:
  - sales
  - finance
  - marketing
  - operations
  - hr
  # Add engagement-specific domains
```

---

## 4. Model Structure

### 4.1 Config Block

Every model must have a config block as the first element:

```sql
{{ config(
    materialized='{view|table|incremental|ephemeral}',
    schema='{target_schema}',
    tags=['{wf_tag}', '{freq_tag}', '{domain_tag}'],
    meta={
        'source_workflow': '{informatica_workflow_name}',
        'target_table': '{original_target_table}',
        'generated_by': 'INFA2DBT_accelerator_v1.1',
        'generation_timestamp': '{iso_timestamp}'
    }
) }}
```

### 4.2 [CUSTOMIZE] Materialization Rules

| Model Type | Default | Override Conditions |
|------------|---------|---------------------|
| `stg_*` | `view` | `table` if source is slow external table |
| `int_*` | `ephemeral` | `view` if referenced 3+ times |
| `mart_*` | `table` | `incremental` if Update Strategy present |

### 4.3 Required Tags

| Tag Type | Format | Required |
|----------|--------|----------|
| Workflow | `wf_{workflow_name}` | Yes |
| Frequency | `{freq_hint}` or `TODO_freq` | Yes |
| Domain | `{domain}` or `TODO_domain` | Yes |
| Review pending | `review_pending` | Only if quarantined |

### 4.4 CTE Structure

All models must follow the CTE-based structure:

```sql
{{ config(...) }}

-- cluster_by = ['TODO: add cluster columns based on downstream query patterns']

WITH source_{table_1} AS (
    SELECT * FROM {{ source('{schema}', '{table_1}') }}
),

source_{table_2} AS (
    SELECT * FROM {{ source('{schema}', '{table_2}') }}
),

filtered_{name} AS (
    SELECT
        col_1,
        col_2,
        col_3
    FROM source_{table_1}
    WHERE {filter_condition}
),

joined_{entities} AS (
    SELECT
        a.col_1,
        a.col_2,
        b.col_3
    FROM filtered_{name} a
    LEFT JOIN source_{table_2} b
        ON a.key_col = b.key_col
),

final AS (
    SELECT
        col_1,
        col_2,
        col_3
    FROM joined_{entities}
)

SELECT * FROM final
```

**Rules:**
- `SELECT *` allowed only in source CTEs and terminal SELECT
- All intermediate CTEs must explicitly list columns
- Final CTE must be named `final`
- Terminal statement must be `SELECT * FROM final`

---

## 5. SQL Style

### 5.1 Formatting

- **Keywords:** UPPERCASE (`SELECT`, `FROM`, `WHERE`, `JOIN`)
- **Identifiers:** lowercase (`customer_id`, `order_date`)
- **Indentation:** 4 spaces
- **Commas:** Leading (at start of line)
- **Line length:** Max 100 characters

### 5.2 Example

```sql
SELECT
    customer_id
    , customer_name
    , CASE
        WHEN status = 'ACTIVE' THEN 1
        ELSE 0
      END AS is_active
    , created_at
FROM source_customers
WHERE is_deleted = FALSE
```

### 5.3 Prohibited Patterns

| Pattern | Why | Alternative |
|---------|-----|-------------|
| Hardcoded schema.table | Breaks environment promotion | Use `{{ source() }}` or `{{ ref() }}` |
| `SELECT *` in intermediate CTEs | Hidden column changes | Explicit column list |
| Inline subqueries in final SELECT | Readability | Separate CTE |
| `QUALIFY` without window function | Snowflake-specific pitfall | Use CTE with window + WHERE |

---

## 6. Documentation

### 6.1 Schema.yml Structure

Every model must have a companion schema.yml entry:

```yaml
version: 2

models:
  - name: mart_sales_summary
    description: >
      Daily sales summary aggregated from ERP order data.
      Converted from Informatica workflow WF_SALES_DAILY.
    meta:
      source_workflow: WF_SALES_DAILY
      target_table: EDW.SALES_SUMMARY
      conversion_timestamp: '2026-03-15T14:00:00Z'
    columns:
      - name: sales_date
        description: "Source: erp.orders.order_date via Expression (DATE_TRUNC)."
        tests:
          - not_null
      - name: region_id
        description: "Source: erp.orders.region_code via Lookup (DIM_REGION)."
        tests:
          - not_null
          - relationships:
              to: ref('dim_region')
              field: region_id
      - name: total_amount
        description: "Source: erp.orders.amount via Aggregator (SUM)."
        tests:
          - not_null
```

### 6.2 Column Description Format

```
"Source: {source_table}.{source_column} via {transformation_type} ({detail})."
```

---

## 7. Testing

### 7.1 Required Tests by Column Type

| Column Type | Required Tests |
|-------------|----------------|
| Primary key | `not_null`, `unique` |
| Foreign key | `not_null`, `relationships` |
| Amount/Quantity | `not_null` (if business rule) |
| Status/Flag | `accepted_values` |
| Date | `not_null` (if required) |

### 7.2 Unit Tests

Every model with transformations must have unit tests covering:

| Transformation | Test Cases |
|---------------|------------|
| Filter | Include matching, exclude non-matching, NULL handling |
| Aggregator | Multi-row group, single row, NULL in aggregate |
| Lookup | Match found, no match, multiple matches |
| Joiner | Inner/outer behavior, NULL keys |
| Router | Each output group |

### 7.3 [CUSTOMIZE] Test Coverage Threshold

```yaml
test_coverage:
  minimum_pct: 80
  required_for_deployment: true
```

---

## 8. Incremental Models

### 8.1 Required Config

```sql
{{ config(
    materialized='incremental',
    unique_key='pk_column',
    merge_update_columns=['col_1', 'col_2'],
    on_schema_change='sync_all_columns'
) }}
```

### 8.2 Incremental Logic

```sql
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### 8.3 [CUSTOMIZE] Incremental Strategy

| Strategy | Use When |
|----------|----------|
| `merge` | Updates to existing rows expected |
| `delete+insert` | Full refresh of changed partitions |
| `append` | Insert-only (no updates) |

---

## 9. Performance

### 9.1 Clustering Keys

```sql
{{ config(
    cluster_by=['date_column', 'high_cardinality_filter_column']
) }}
```

**Selection criteria:**
1. Most common filter columns in downstream queries
2. Date/timestamp columns (for range filters)
3. High-cardinality columns used in joins

### 9.2 [CUSTOMIZE] Query Timeout

```yaml
query_timeout_seconds: 3600
```

---

## 10. Versioning

### 10.1 Guidelines Version

This document version: `2.3`

Models generated with this version include:
```sql
meta={
    'coding_guidelines_version': '2.3'
}
```

### 10.2 Changelog

| Version | Date | Changes |
|---------|------|---------|
| 2.3 | 2026-03-15 | Added INFA2DBT meta requirements |
| 2.2 | 2026-02-01 | Updated CTE naming conventions |
| 2.1 | 2026-01-15 | Added unit test requirements |
| 2.0 | 2025-12-01 | Initial release |

---

## Appendix A: Customization Checklist

Before using these guidelines, customize the following:

- [ ] Section 3.2: Define domain tags
- [ ] Section 4.2: Adjust materialization rules
- [ ] Section 7.3: Set test coverage threshold
- [ ] Section 8.3: Choose incremental strategy
- [ ] Section 9.2: Set query timeout
