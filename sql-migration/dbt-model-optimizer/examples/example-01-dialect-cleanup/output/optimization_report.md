# dbt-model-optimizer — Optimization Report

**Model:** `model_with_teradata.sql`
**Detected dialect:** Oracle (NVL, DECODE, SYSDATE, SUBSTR detected)
**Snowflake connection:** Unavailable (D5/D6 skipped)

---

## Summary

| Dimension | SAFE | REVIEW | REQUIRES APPROVAL | Total |
|-----------|------|--------|-------------------|-------|
| D1: Dialect Cleanup | 5 | 0 | 0 | 5 |
| D2: Code Quality | 7 | 0 | 0 | 7 |
| D3: Snowflake-Native | 0 | 0 | 0 | 0 |
| D4: Testing/Docs | 1 | 0 | 0 | 1 |
| D5: Performance | — | — | — | Skipped |
| D6: Cost | — | — | — | Skipped |
| **Total** | **13** | **0** | **0** | **13** |

---

## SAFE Changes (Applied)

### D1: Dialect Cleanup

| # | Rule ID | Line | Original | Replacement |
|---|---------|------|----------|-------------|
| 1 | D1-ORA-001 | 9 | `NVL(discount_amount, 0)` | `COALESCE(discount_amount, 0)` |
| 2 | D1-ORA-001 | 10 | `NVL(tax_amount, 0)` | `COALESCE(tax_amount, 0)` |
| 3 | D1-ORA-003 | 11 | `DECODE(status_code, 'A', 'Active', ...)` | `CASE WHEN status_code = 'A' THEN 'Active' ... END` |
| 4 | D1-ORA-008 | 12 | `SUBSTR(region_code, 1, 2)` | `SUBSTRING(region_code, 1, 2)` |
| 5 | D1-ORA-004 | 13 | `SYSDATE` | `CURRENT_DATE()` |

### D2: Code Quality

| # | Rule ID | Line | Description | Fix |
|---|---------|------|-------------|-----|
| 6 | D2-CFG-005 | 1 | Missing `query_tag` in config | Added `query_tag='n_order_header'` |
| 7 | D2-DEP-001 | 16 | Raw table reference `$$STGDB.RAW_SALES.N_ORDER_HEADER` | Replaced with `{{ source('raw_sales', 'N_ORDER_HEADER') }}` |
| 8 | D1-INF-001 | 16 | Informatica variable `$$STGDB` | Removed (absorbed into source() macro) |
| 9 | D2-AUDIT-001 | 30-33 | `EDW_*` audit column names | Renamed to `EDWSF_*` |
| 10 | D2-AUDIT-002 | — | Missing `EDWSF_BATCH_ID` (N_ prefix model) | Added `{{ var('batch_id', 0) }} AS EDWSF_BATCH_ID` |
| 11 | D2-AUDIT-003 | 30-31 | Audit DTM columns not using `CURRENT_TIMESTAMP(0)` | Added `CURRENT_TIMESTAMP(0) AS EDWSF_CREATE_DTM` etc. |
| 12 | D2-AUDIT-004 | 32-33 | Audit USER columns not using `CAST(CURRENT_USER()...)` | Added `CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_CREATE_USER` etc. |

### D4: Testing/Docs

| # | Rule ID | Description | Fix |
|---|---------|-------------|-----|
| 13 | D4-MISS-001 | No schema.yml found | Generated complete schema.yml stub (see below) |

---

## Validation

- **dbt compile:** Unavailable (no dbt project root found)
- **Syntax check:** Passed — no unclosed parentheses or unmatched quotes
- **Live profiling:** Skipped (no Snowflake connection)

---

## Next Steps

1. Review the optimized model file
2. Place the generated schema.yml stub in the model directory
3. Connect to Snowflake and re-run for D5/D6 analysis
4. Set up dbt project root for full compile/test validation
