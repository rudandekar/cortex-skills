# Snowflake-Native Pattern Catalog

Reference document for `dbt-model-optimizer` Step 4 (Dimension D3). Contains
before/after SQL examples for each Snowflake-native optimization pattern.

---

## Table of Contents

1. [QUALIFY Adoption](#qualify-adoption)
2. [LATERAL FLATTEN for JSON/VARIANT](#lateral-flatten-for-jsonvariant)
3. [SPLIT_TO_TABLE for String Splitting](#split_to_table-for-string-splitting)
4. [MATCH_RECOGNIZE for Sessionization](#match_recognize-for-sessionization)
5. [Dynamic Table Candidacy](#dynamic-table-candidacy)
6. [DECODE Style Preference](#decode-style-preference)
7. [Other Snowflake-Specific Features](#other-snowflake-specific-features)

---

## QUALIFY Adoption

**Rule ID:** D3-QUAL-001
**Safety:** REVIEW — semantically equivalent but user should verify window specification

### Pattern: ROW_NUMBER + WHERE filter

**Before:**
```sql
WITH ranked AS (
    SELECT
        customer_id,
        order_date,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM {{ ref('stg_orders') }}
)
SELECT
    customer_id,
    order_date,
    amount
FROM ranked
WHERE rn = 1
```

**After:**
```sql
SELECT
    customer_id,
    order_date,
    amount
FROM {{ ref('stg_orders') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
```

### Pattern: RANK + WHERE filter

**Before:**
```sql
WITH deduped AS (
    SELECT *,
        RANK() OVER (PARTITION BY account_id ORDER BY effective_date DESC) AS rnk
    FROM {{ source('raw', 'accounts') }}
)
SELECT * FROM deduped WHERE rnk = 1
```

**After:**
```sql
SELECT *
FROM {{ source('raw', 'accounts') }}
QUALIFY RANK() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
```

### Detection heuristic:
1. CTE assigns `ROW_NUMBER()`, `RANK()`, or `DENSE_RANK()` to an alias (commonly `rn`, `rnk`, `row_num`)
2. Downstream CTE or final SELECT filters `WHERE {alias} = 1` (or `<= N`)
3. The window function and filter can be collapsed into the source query with QUALIFY

### When NOT to apply:
- The rank alias is used for more than just filtering (e.g., displayed as a column)
- Multiple rank columns with different window specs are used together
- The CTE has other transformations besides the window function

---

## LATERAL FLATTEN for JSON/VARIANT

**Rule ID:** D3-FLAT-001
**Safety:** REVIEW — requires understanding of the JSON structure

### Pattern: Nested VARIANT access via subquery

**Before:**
```sql
SELECT
    raw.id,
    raw.payload:customer:name::VARCHAR AS customer_name,
    item.value:product_id::NUMBER AS product_id,
    item.value:quantity::NUMBER AS quantity
FROM {{ source('raw', 'events') }} raw,
    TABLE(FLATTEN(raw.payload:items)) item
```

This is already using FLATTEN — no change needed. The pattern to detect is when
the code uses subqueries or CTEs to unnest arrays instead:

**Before (subquery approach):**
```sql
WITH base AS (
    SELECT id, payload
    FROM {{ source('raw', 'events') }}
),
items AS (
    SELECT
        b.id,
        b.payload:customer:name::VARCHAR AS customer_name,
        f.value AS item
    FROM base b,
        TABLE(FLATTEN(input => b.payload, path => 'items')) f
),
final AS (
    SELECT
        id,
        customer_name,
        item:product_id::NUMBER AS product_id,
        item:quantity::NUMBER AS quantity
    FROM items
)
SELECT * FROM final
```

**After (LATERAL FLATTEN — more concise):**
```sql
SELECT
    raw.id,
    raw.payload:customer:name::VARCHAR AS customer_name,
    f.value:product_id::NUMBER AS product_id,
    f.value:quantity::NUMBER AS quantity
FROM {{ source('raw', 'events') }} raw,
    LATERAL FLATTEN(input => raw.payload:items) f
```

### Detection heuristic:
1. CTE chain with intermediate CTE that only does FLATTEN
2. Subsequent CTE that only extracts fields from the flattened value
3. Can be collapsed into a single query with LATERAL FLATTEN

---

## SPLIT_TO_TABLE for String Splitting

**Rule ID:** D3-SPLIT-001
**Safety:** REVIEW — complex rewrite

### Pattern: STRTOK loop or recursive CTE for splitting

**Before (STRTOK approach):**
```sql
WITH split AS (
    SELECT
        id,
        TRIM(STRTOK(tag_list, ',', seq.seq_num)) AS tag
    FROM {{ ref('products') }}
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS seq_num
        FROM TABLE(GENERATOR(ROWCOUNT => 100))
    ) seq
    WHERE STRTOK(tag_list, ',', seq.seq_num) IS NOT NULL
)
SELECT * FROM split
```

**After:**
```sql
SELECT
    p.id,
    TRIM(s.value) AS tag
FROM {{ ref('products') }} p,
    LATERAL SPLIT_TO_TABLE(p.tag_list, ',') s
```

### Detection heuristic:
1. `STRTOK` with a sequence/generator cross join
2. Recursive CTE for string splitting
3. `REGEXP_SUBSTR` with iterative extraction

---

## MATCH_RECOGNIZE for Sessionization

**Rule ID:** D3-MATCH-001
**Safety:** REQUIRES_APPROVAL — complex rewrite with semantic implications

### Pattern: Window function sessionization

**Before:**
```sql
WITH gaps AS (
    SELECT
        user_id,
        event_time,
        DATEDIFF('minute', LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time), event_time) AS gap_minutes,
        CASE WHEN gap_minutes > 30 OR gap_minutes IS NULL THEN 1 ELSE 0 END AS new_session
    FROM {{ ref('events') }}
),
sessions AS (
    SELECT
        user_id,
        event_time,
        SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
    FROM gaps
)
SELECT * FROM sessions
```

**After (MATCH_RECOGNIZE):**
```sql
SELECT *
FROM {{ ref('events') }}
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY event_time
    MEASURES
        MATCH_NUMBER() AS session_id,
        FIRST(event_time) AS session_start,
        LAST(event_time) AS session_end
    ONE ROW PER MATCH
    PATTERN (session_start continued*)
    DEFINE
        continued AS DATEDIFF('minute', LAG(event_time), event_time) <= 30
)
```

Note: This is an advanced optimization. Only recommend for Tier 1 models with complex sessionization logic.

---

## Dynamic Table Candidacy

**Rule ID:** D3-DT-001
**Safety:** REQUIRES_APPROVAL — architectural change

### Eligibility criteria (ALL must be true):
1. `materialized='table'` (not incremental, not view)
2. No `pre_hook` or `post_hook` in config
3. No `{{ this }}` self-reference in SQL body
4. Pure SELECT transform (no side effects, no DML)
5. All upstream sources are tables (not views or ephemeral models)
6. Model is refreshed on a schedule (not ad-hoc)

### Recommendation format:
```sql
-- Current
{{ config(
    materialized='table',
    schema='mart'
) }}

-- Recommended (Dynamic Table)
{{ config(
    materialized='dynamic_table',
    schema='mart',
    target_lag='1 hour',  -- adjust based on freshness requirements
    snowflake_warehouse='TRANSFORM_WH'
) }}
```

### target_lag guidance:
| Freshness Requirement | Recommended target_lag |
|----------------------|----------------------|
| Near real-time | `'1 minute'` to `'5 minutes'` |
| Hourly reporting | `'1 hour'` |
| Daily reporting | `'1 day'` |
| Infrequent access | `'DOWNSTREAM'` (refresh only when downstream needs it) |

---

## DECODE Style Preference

**Rule ID:** D3-DEC-001
**Safety:** REVIEW — optional style preference, semantically equivalent

### Pattern: CASE WHEN with 4+ equality branches on same column

**Before:**
```sql
CASE region_code
    WHEN 'NA' THEN 'North America'
    WHEN 'EU' THEN 'Europe'
    WHEN 'AP' THEN 'Asia Pacific'
    WHEN 'LA' THEN 'Latin America'
    WHEN 'AF' THEN 'Africa'
    ELSE 'Unknown'
END AS region_name
```

**After:**
```sql
DECODE(region_code,
    'NA', 'North America',
    'EU', 'Europe',
    'AP', 'Asia Pacific',
    'LA', 'Latin America',
    'AF', 'Africa',
    'Unknown'
) AS region_name
```

Note: This is purely a style preference. Snowflake supports both syntaxes natively.
Only suggest when there are 4 or more branches. Mark as REVIEW — the user may prefer
CASE WHEN for readability.

---

## Other Snowflake-Specific Features

These are noted for awareness but not actively recommended by the optimizer:

| Feature | Use Case | Notes |
|---------|----------|-------|
| `SEARCH OPTIMIZATION SERVICE` | Point lookups on large tables | Requires D5 analysis — recommend if point lookups detected |
| `MATERIALIZED VIEWS` | Frequently queried aggregations | Limited in Snowflake — no joins, only aggregations |
| `RESULT_SCAN()` | Chaining query results | Avoid in dbt models — use CTEs instead |
| `IDENTIFIER()` | Dynamic SQL in procedures | Not applicable to dbt models |
| `GENERATOR()` | Sequence generation | Use `SPLIT_TO_TABLE` or `FLATTEN` instead where possible |
