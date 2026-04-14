# Optimization Rules Master Catalog

Reference document for `dbt-model-optimizer`. Consolidated from assessment_framework.yaml
(24 rules), dbt-validation-critique checks, and dbt-sql-optimizer patterns.

---

## Table of Contents

1. [D1 — Dialect Cleanup Rules](#d1--dialect-cleanup-rules)
2. [D2 — Code Quality Rules](#d2--code-quality-rules)
3. [D3 — Snowflake-Native Pattern Rules](#d3--snowflake-native-pattern-rules)
4. [D4 — Testing and Documentation Rules](#d4--testing-and-documentation-rules)
5. [D5 — Performance Rules](#d5--performance-rules)
6. [D6 — Cost Optimization Rules](#d6--cost-optimization-rules)

---

## D1 — Dialect Cleanup Rules

See `dialect-patterns.md` for the full pattern catalog. Summary:

| Rule ID Range | Count | Description | Default Safety |
|---------------|-------|-------------|----------------|
| D1-ORA-001 to D1-ORA-021 | 17 | Oracle → Snowflake | SAFE (except ROWNUM, outer join, CONNECT BY) |
| D1-TD-001 to D1-TD-030 | 19 | Teradata → Snowflake | SAFE (except volatile tables, UPDATE FROM) |
| D1-SS-001 to D1-SS-040 | 25 | SQL Server → Snowflake | SAFE (except identity, temp tables, control flow) |
| D1-XD-001 to D1-XD-003 | 3 | Cross-dialect | REVIEW |
| D1-INF-001 to D1-INF-005 | 5 | Informatica variables | SAFE/REVIEW |

---

## D2 — Code Quality Rules

Absorbed from: assessment_framework.yaml checks 1-24, dbt-validation-critique checks.

### Config Block Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D2-CFG-001 | AF-7 | `{{ config() }}` block missing | HIGH | SAFE | Yes — generate config block |
| D2-CFG-002 | — | `materialized` not set in config | MEDIUM | SAFE | Yes — add based on model analysis |
| D2-CFG-003 | — | `schema` not set in config | MEDIUM | SAFE | Yes — infer from model path |
| D2-CFG-004 | AF-22 | `database=var()` missing for cross-database model | MEDIUM | SAFE | Yes |
| D2-CFG-005 | — | `query_tag` not present in config | LOW | SAFE | Yes — use model name |
| D2-CFG-006 | — | `on_schema_change` missing for incremental model | MEDIUM | SAFE | Yes — default `'append_new_columns'` |
| D2-CFG-007 | — | `incremental_strategy` missing for incremental model | MEDIUM | SAFE | Yes — default `'append'` |
| D2-CFG-008 | AF-21 | Wrong materialization (table when hooks present → should be incremental) | HIGH | REQUIRES_APPROVAL | No |

### CTE Structure Rules

| Rule ID | Source | Description | Severity | Safety | Auto-fix |
|---------|--------|-------------|----------|--------|----------|
| D2-CTE-001 | 7B-A | Unused CTE (defined but not referenced) | MEDIUM | SAFE | Yes — remove CTE |
| D2-CTE-002 | 7B-C | Redundant CTEs (identical SQL body) | MEDIUM | REVIEW | No — consolidation changes references |
| D2-CTE-003 | — | Inline subquery in final SELECT should be CTE | LOW | REVIEW | No — restructure needed |

### Dependency Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D2-DEP-001 | AF-6 | Raw table reference instead of source()/ref() | HIGH | SAFE | Yes — generate macro call |
| D2-DEP-002 | AF-11, Guardrail | W_/WI_/ST_ etc. using source() instead of ref() | HIGH | SAFE | Yes — replace with ref() |
| D2-DEP-003 | AF-10 | N_/MT_/R_ external table using ref() instead of source() | HIGH | SAFE | Yes — replace with source() |
| D2-DEP-004 | Guardrail | Incremental model without {{ this }} for self-ref | HIGH | SAFE | Yes — replace target table ref |

### Hook Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D2-HOOK-001 | AF-12 | Pre-SQL exists but no pre_hook in config | HIGH | SAFE | Yes — generate pre_hook |
| D2-HOOK-002 | AF-13 | Post-SQL exists but no post_hook in config | HIGH | SAFE | Yes — generate post_hook |
| D2-HOOK-003 | AF-19 | Session SQL exists but not captured in hooks | HIGH | SAFE | Yes — add to hook array |
| D2-HOOK-004 | AF-24 | Wrong PST timestamp macro name | MEDIUM | SAFE | Yes — replace with {{ edw_convert_to_pst() }} |

### Audit Column Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D2-AUDIT-001 | AF-20 | EDW_* naming instead of EDWSF_* | HIGH | SAFE | Yes — rename |
| D2-AUDIT-002 | AF-20 | N_/MT_ missing EDWSF_BATCH_ID | HIGH | SAFE | Yes — add column |
| D2-AUDIT-003 | AF-20/Guardrail | MT_ not using CURRENT_TIMESTAMP(0) for DTM columns | HIGH | SAFE | Yes — fix expression |
| D2-AUDIT-004 | AF-20/Guardrail | MT_ not using CAST(CURRENT_USER() AS VARCHAR(30)) for USER columns | HIGH | SAFE | Yes — fix expression |
| D2-AUDIT-005 | AF-20/Guardrail | edw_convert_to_pst() used in SELECT (only valid in hooks) | HIGH | SAFE | Yes — replace with CURRENT_TIMESTAMP(0) |
| D2-AUDIT-006 | AF-23 | Hook SQL references EDW_* instead of EDWSF_* | HIGH | SAFE | Yes — rename in hook |

### Other Code Quality Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D2-PARSE-001 | AF-3 | SQL parse failure | CRITICAL | — | No — blocks analysis |
| D2-LINT-001 | AF-4 | SQL lint violations | LOW | SAFE | Yes — auto-format |
| D2-STRUCT-001 | AF-5 | Target column missing from SQL | HIGH | REVIEW | No — business logic needed |
| D2-STRUCT-002 | AF-8 | SQL model count mismatch with target count | HIGH | REVIEW | No |
| D2-READ-001 | AF-15 | Multiple sources with no UNION or ref() combiner | MEDIUM | REVIEW | No |
| D2-IMPL-001 | AF-16 | Update/upsert strategy but no MERGE statement | CRITICAL | REVIEW | No — complex rewrite |
| D2-IMPL-002 | AF-17 | UPDATE SET columns in post_hook instead of MERGE | HIGH | REVIEW | No — MERGE restructure |

---

## D3 — Snowflake-Native Pattern Rules

| Rule ID | Description | Severity | Safety | Auto-fix |
|---------|-------------|----------|--------|----------|
| D3-QUAL-001 | ROW_NUMBER + WHERE rn = 1 → QUALIFY | MEDIUM | REVIEW | Yes — but user should verify window spec |
| D3-FLAT-001 | Nested VARIANT access → LATERAL FLATTEN | MEDIUM | REVIEW | No — requires understanding JSON structure |
| D3-SPLIT-001 | String splitting loop → SPLIT_TO_TABLE | LOW | REVIEW | No — complex rewrite |
| D3-DT-001 | Pure transform model → Dynamic Table candidate | LOW | REQUIRES_APPROVAL | No — architectural change |
| D3-DEC-001 | CASE with 4+ branches → DECODE (style) | LOW | REVIEW | Yes — optional preference |
| D3-MATCH-001 | Sessionization logic → MATCH_RECOGNIZE candidate | LOW | REQUIRES_APPROVAL | No — complex rewrite |

---

## D4 — Testing and Documentation Rules

| Rule ID | Source Check# | Description | Severity | Safety | Auto-fix |
|---------|--------------|-------------|----------|--------|----------|
| D4-DOC-001 | AF-9 | Column not documented in schema.yml | MEDIUM | SAFE | Yes — generate stub |
| D4-DOC-002 | — | Model description empty or missing | MEDIUM | SAFE | Yes — placeholder |
| D4-DOC-003 | — | Column description empty | LOW | SAFE | Yes — placeholder |
| D4-TEST-001 | AF-18 | NOT NULL column missing not_null test | MEDIUM | SAFE | Yes — add test |
| D4-TEST-002 | AF-18 | PK column missing unique test | MEDIUM | SAFE | Yes — add test |
| D4-UNIT-001 | — | No unit test file found | LOW | REVIEW | No — cannot auto-generate |
| D4-MISS-001 | — | No schema.yml found at all | HIGH | SAFE | Yes — generate complete stub |

---

## D5 — Performance Rules

| Rule ID | Description | Severity | Safety | Auto-fix |
|---------|-------------|----------|--------|----------|
| D5-CLUST-001 | Add clustering keys (low partition pruning) | MEDIUM | REQUIRES_APPROVAL | No |
| D5-CLUST-002 | Clustering key exceeds 4 columns | LOW | REQUIRES_APPROVAL | No |
| D5-SPILL-001 | Query spilling to local storage | HIGH | REQUIRES_APPROVAL | No |
| D5-SPILL-002 | Query spilling to remote storage | CRITICAL | REQUIRES_APPROVAL | No |
| D5-PRUNE-001 | Partition pruning ratio below 50% | MEDIUM | REQUIRES_APPROVAL | No |
| D5-PLAN-001 | Nested loop join detected (potential cartesian) | HIGH | REVIEW | No |

---

## D6 — Cost Optimization Rules

| Rule ID | Description | Severity | Safety | Auto-fix |
|---------|-------------|----------|--------|----------|
| D6-MAT-001 | Downgrade table to view (low query frequency) | MEDIUM | REQUIRES_APPROVAL | No |
| D6-MAT-002 | Upgrade view to table (high query frequency) | MEDIUM | REQUIRES_APPROVAL | No |
| D6-MAT-003 | Convert to incremental (append pattern) | MEDIUM | REQUIRES_APPROVAL | No |
| D6-MAT-004 | Dynamic Table candidate | LOW | REQUIRES_APPROVAL | No |
| D6-WH-001 | Upsize warehouse (spilling detected) | MEDIUM | REQUIRES_APPROVAL | No |
| D6-WH-002 | Downsize warehouse (underutilized) | MEDIUM | REQUIRES_APPROVAL | No |
| D6-STORE-001 | Unused table consuming storage | LOW | REQUIRES_APPROVAL | No |
