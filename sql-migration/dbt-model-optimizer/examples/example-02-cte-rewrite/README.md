# Example 02 — CTE Rewrite (Unused CTEs, QUALIFY, Missing Tests)

## What this demonstrates

A post-migration dbt model with CTE structural issues: an unused CTE, a
ROW_NUMBER + WHERE rn = 1 pattern that should use QUALIFY, and missing test
coverage in schema.yml.

## Input characteristics

- Unused CTE: `unused_lookup` is defined but never referenced
- QUALIFY opportunity: `ranked` CTE with ROW_NUMBER → `filtered` CTE with WHERE rn = 1
- schema.yml exists but is incomplete (missing columns, no tests)
- Config block is mostly complete but missing `query_tag`
- No audit column issues (clean EDWSF_* naming)

## Notable decisions in the output

- Unused CTE removal is SAFE (no semantic change)
- QUALIFY rewrite is REVIEW (user should verify window spec is correct)
- Missing column documentation is SAFE (additive — generates stubs)
- Missing not_null/unique tests are SAFE (additive — generates test entries)
- The optimizer does NOT auto-apply REVIEW items — they appear as recommendations

## Known gaps

- Does not demonstrate dialect cleanup (see Example 01)
- Does not demonstrate hook configuration or audit column patterns
- Does not demonstrate live profiling (D5/D6)
