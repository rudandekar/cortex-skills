# dbt-model-optimizer — Optimization Report

**Model:** `model_with_redundant_ctes.sql`
**Detected dialect:** Clean Snowflake (no legacy syntax detected)
**Snowflake connection:** Unavailable (D5/D6 skipped)

---

## Summary

| Dimension | SAFE | REVIEW | REQUIRES APPROVAL | Total |
|-----------|------|--------|-------------------|-------|
| D1: Dialect Cleanup | 0 | 0 | 0 | 0 |
| D2: Code Quality | 2 | 0 | 0 | 2 |
| D3: Snowflake-Native | 0 | 1 | 0 | 1 |
| D4: Testing/Docs | 3 | 1 | 0 | 4 |
| D5: Performance | — | — | — | Skipped |
| D6: Cost | — | — | — | Skipped |
| **Total** | **5** | **2** | **0** | **7** |

---

## SAFE Changes (Applied)

### D2: Code Quality

| # | Rule ID | Line | Description | Fix |
|---|---------|------|-------------|-----|
| 1 | D2-CTE-001 | 19-26 | Unused CTE `unused_lookup` (defined but never referenced) | Removed CTE |
| 2 | D2-CFG-005 | 1 | Missing `query_tag` in config | Added `query_tag='model_with_redundant_ctes'` |

### D4: Testing/Docs

| # | Rule ID | Description | Fix |
|---|---------|-------------|-----|
| 3 | D4-TEST-001 | `order_id` appears to be PK — missing `not_null` test | Added not_null test to schema.yml |
| 4 | D4-TEST-002 | `order_id` appears to be PK — missing `unique` test | Added unique test to schema.yml |
| 5 | D4-DOC-003 | Multiple columns missing descriptions in schema.yml | Generated placeholder descriptions |

---

## REVIEW Items (Not Applied — User Should Evaluate)

### D3: Snowflake-Native

**D3-QUAL-001** — QUALIFY adoption (lines 28-48)

The `ranked` CTE assigns `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn`, and the `filtered` CTE selects `WHERE rn = 1`. This can be collapsed into a single query using Snowflake's QUALIFY clause.

**Before:**
```sql
ranked AS (
    SELECT ..., ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM source_orders
),
filtered AS (
    SELECT ... FROM ranked WHERE rn = 1
)
```

**After:**
```sql
-- Remove ranked and filtered CTEs, add to source_orders or final:
FROM source_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
```

**Rationale:** Reduces CTE count by 2, improves readability, uses Snowflake-native syntax. Semantically equivalent — verify the window specification is correct.

### D4: Testing/Docs

**D4-UNIT-001** — No unit test file found for this model. Consider creating `tests/unit/test_model_with_redundant_ctes.sql`.

---

## Validation

- **dbt compile:** Unavailable (no dbt project root found)
- **Syntax check:** Passed
- **Live profiling:** Skipped (no Snowflake connection)

---

## Next Steps

1. Review the QUALIFY rewrite recommendation (D3-QUAL-001) — apply if window spec is correct
2. Create a unit test for this model
3. Connect to Snowflake and re-run for D5/D6 analysis
