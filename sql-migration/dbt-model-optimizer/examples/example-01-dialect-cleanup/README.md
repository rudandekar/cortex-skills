# Example 01 — Dialect Cleanup (Oracle/Teradata Artifacts)

## What this demonstrates

A post-migration dbt model that was converted from an Informatica mapping originally
sourcing from Oracle and Teradata systems. The model contains residual legacy function
calls (NVL, DECODE, SYSDATE, SUBSTR), Informatica variable references, incorrect
audit column naming (EDW_* instead of EDWSF_*), and missing config elements.

## Input characteristics

- Oracle functions: `NVL()`, `DECODE()`, `SYSDATE`, `SUBSTR()`
- Informatica variable: `$$STGDB`
- Incorrect audit columns: `EDW_CREATE_DTM` etc.
- Missing `query_tag` in config
- Missing `EDWSF_BATCH_ID` for N_ prefix model
- No schema.yml (tests/docs missing)

## Notable decisions in the output

- All dialect function replacements are classified as SAFE and auto-applied
- `$$STGDB` replacement is SAFE (maps to var())
- Audit column renaming is SAFE (deterministic rename)
- Missing schema.yml generates a complete stub (SAFE)
- DECODE replacement uses CASE WHEN for clarity (not Snowflake's native DECODE)

## Known gaps

- Does not demonstrate D5/D6 live profiling (no Snowflake connection in example)
- Does not demonstrate QUALIFY rewrite (see Example 02)
- Does not demonstrate incremental model hook patterns
