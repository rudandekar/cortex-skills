# dbt Compile Report — mart_comp_time_daily_tbl

**Project:** example_infa2dbt_project
**Selector:** `mart_comp_time_daily_tbl`
**Timestamp:** 2026-03-31T15:30:00Z

## Summary

| Metric | Value |
|--------|-------|
| Models compiled | 1/1 |
| Compile errors | 0 |
| Warnings | 5 |
| Snowflake validated | No (not requested) |

## Model: mart_comp_time_daily_tbl

**Status:** Compiled successfully with warnings

### Structure
- **CTEs:** 5 (source_u0287d01, source_pay_period, transformed_exp_build_pay_period, transformed_exp_final, final)
- **Sources resolved:** RAW.PUBLIC.U0287D01, RAW.PUBLIC.PAY_PERIOD
- **Refs resolved:** None
- **Final columns:** 15

### Informatica Artifacts (requires manual fix)

| Line | Artifact | Severity | Detail |
|------|----------|----------|--------|
| 51 | `SETVARIABLE($$MAP_PP_YEAR_NUM, ...)` | Error | Not valid Snowflake SQL — convert to dbt `var()` or remove |
| 52 | `SETVARIABLE($$MAP_PP_END_YEAR, ...)` | Error | Not valid Snowflake SQL — convert to dbt `var()` or remove |
| 53 | `SETVARIABLE($$MAP_PP_NUM, ...)` | Error | Not valid Snowflake SQL — convert to dbt `var()` or remove |
| 50 | `v_PP_NUM` | Warning | Unconverted Informatica variable reference |

### TODO Comments

- Line 27: `Note: 2 additional columns omitted - review source schema`

## Next Steps

1. Convert `SETVARIABLE()` calls to dbt `var()` or Snowflake session variables
2. Resolve `v_PP_NUM` — replace with actual column reference or dbt variable
3. Review omitted columns (line 27) against source schema
4. Run with `--validate-snowflake` after fixing artifacts to validate against Snowflake
