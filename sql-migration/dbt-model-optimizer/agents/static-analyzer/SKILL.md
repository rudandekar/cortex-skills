---
name: static-analyzer
version: 0.1.0
tier: project
author: rudandekar
created: 2026-04-07
last_updated: 2026-04-07
status: draft

description: >
  Sub-agent for dbt-model-optimizer. Performs static analysis across four dimensions:
  D1 (dialect cleanup), D2 (code quality), D3 (Snowflake-native patterns), and
  D4 (testing/documentation). No Snowflake connection required. Invoked by the
  parent orchestrator — do not invoke directly.
---

# Static Analyzer (Sub-Agent A)

Performs pure file-based analysis of a single dbt model across dimensions D1-D4.
No Snowflake connection or dbt compilation required. Returns a structured list of
findings for the parent orchestrator to categorize and apply.

## Inputs

- **Required:** `model_content` — full text content of the .sql model file
- **Required:** `model_name` — filename without extension (e.g., `N_IAM_SALES_TERR_LNK_TV`)
- **Required:** `detected_dialect` — one of `teradata`, `oracle`, `sqlserver`, `clean_snowflake`
- **Optional:** `schema_yml_content` — full text of the sibling schema.yml file (if found)
- **Optional:** `dbt_project_yml_content` — full text of dbt_project.yml (if found)
- **Optional:** `table_prefix` — extracted from model_name (e.g., `N_`, `MT_`, `WI_`, `ST_`)

## Workflow

### Step A.1: Parse model structure

1. Extract the `{{ config(...) }}` block if present. Parse its keys: `materialized`, `schema`, `database`, `query_tag`, `pre_hook`, `post_hook`, `on_schema_change`, `incremental_strategy`, `tags`, `meta`.
2. Identify all CTEs: name, SQL body, line range.
3. Identify the final SELECT statement (everything after the last CTE).
4. Extract all column aliases from the final SELECT.
5. Identify all table/view references in FROM/JOIN clauses.
6. Detect `{{ source('...', '...') }}`, `{{ ref('...') }}`, and `{{ this }}` macro usage.
7. Extract audit columns from the final SELECT (any column starting with `EDW` or `EDWSF`).

### Step A.2: Dimension D1 — Dialect cleanup

Load `references/dialect-patterns.md`. For each pattern in the detected dialect section:

1. Scan model_content line by line for matches (case-insensitive).
2. For each match, record: `rule_id`, `line_number`, `original` (matched text), `replacement` (Snowflake equivalent), `safety: "SAFE"`, `auto_fixable: true`.

Additional D1 checks:
- **Informatica variables**: scan for `$$` prefixed tokens → emit finding with replacement note
- **Hardcoded schemas**: scan for patterns like `DATABASE.SCHEMA.TABLE` without `{{ }}` Jinja → emit finding
- **Volatile tables**: `CREATE VOLATILE TABLE` → recommend CTE replacement
- **Transaction markers**: `BT;` / `ET;` → remove (SAFE)
- **Statistics**: `COLLECT STAT` → remove (SAFE)

### Step A.3: Dimension D2 — Code quality

Load `references/dbt-conventions.md`. Check each category:

**Config block** (emit finding per missing/incorrect item):
- `{{ config(...) }}` exists → if missing: `D2-CFG-001`, SAFE
- `materialized` set → if missing: `D2-CFG-002`, SAFE
- `schema` set → if missing: `D2-CFG-003`, SAFE
- `database=var('...')` for cross-database models → if missing: `D2-CFG-004`, SAFE
- `query_tag` present → if missing: `D2-CFG-005`, SAFE
- `on_schema_change` for incremental → if missing: `D2-CFG-006`, SAFE
- `incremental_strategy` for incremental → if missing: `D2-CFG-007`, SAFE

**CTE structure:**
- Unused CTEs (defined but not referenced downstream) → `D2-CTE-001`, SAFE
- Redundant CTEs (identical SQL body) → `D2-CTE-002`, REVIEW (consolidation changes semantics if CTE is referenced with different aliases)
- Inline subqueries in final SELECT → `D2-CTE-003`, REVIEW

**Dependencies:**
- Raw table references (no source/ref macro) → `D2-DEP-001`, SAFE (provide macro replacement)
- Wrong macro for table prefix:
  - W_/WI_/ST_/EX_/EL_/SM_/GL_/FA_ using `source()` instead of `ref()` → `D2-DEP-002`, SAFE
  - N_/MT_/R_ external tables using `ref()` instead of `source()` → `D2-DEP-003`, SAFE
- Incremental model without `{{ this }}` for self-ref → `D2-DEP-004`, SAFE

**Hooks:**
- Pre-SQL indicated but no `pre_hook` → `D2-HOOK-001`, SAFE (generate hook config)
- Post-SQL indicated but no `post_hook` → `D2-HOOK-002`, SAFE
- Session SQL not captured → `D2-HOOK-003`, SAFE
- Wrong macro name (not `{{ edw_convert_to_pst() }}`) → `D2-HOOK-004`, SAFE

**Audit columns:**
- `EDW_*` instead of `EDWSF_*` naming → `D2-AUDIT-001`, SAFE
- N_/MT_ missing `EDWSF_BATCH_ID` → `D2-AUDIT-002`, SAFE
- MT_ not using `CURRENT_TIMESTAMP(0)` for datetime audit columns → `D2-AUDIT-003`, SAFE
- MT_ not using `CAST(CURRENT_USER() AS VARCHAR(30))` for user audit columns → `D2-AUDIT-004`, SAFE
- `edw_convert_to_pst()` used in SELECT column (should only be in hooks) → `D2-AUDIT-005`, SAFE
- Hook SQL references `EDW_*` instead of `EDWSF_*` → `D2-AUDIT-006`, SAFE

### Step A.4: Dimension D3 — Snowflake-native patterns

Load `references/snowflake-native.md`. Check for each pattern:

- **QUALIFY**: `ROW_NUMBER() OVER(...) AS rn` in CTE + `WHERE rn = 1` in next CTE/SELECT → `D3-QUAL-001`, REVIEW (safety: semantically equivalent but user should verify window spec)
- **FLATTEN**: Nested access on VARIANT/OBJECT columns via subquery → `D3-FLAT-001`, REVIEW
- **SPLIT_TO_TABLE**: String splitting via STRTOK loops or recursive CTE → `D3-SPLIT-001`, REVIEW
- **Dynamic Table candidacy**: Model has `materialized='table'`, no hooks, no `{{ this }}`, no pre/post SQL, pure SELECT transform → `D3-DT-001`, REQUIRES_APPROVAL
- **DECODE style**: `CASE col WHEN` with 4+ branches → `D3-DEC-001`, REVIEW (optional style preference)

Each finding includes before_snippet and after_snippet with the rewritten SQL.

### Step A.5: Dimension D4 — Testing and documentation

If `schema_yml_content` is available:
- Parse YAML, find the model entry.
- **Column coverage**: Compare columns from final SELECT (Step A.1) against documented columns. Each undocumented column → `D4-DOC-001`, SAFE (generate stub).
- **Test coverage**: For each column that appears to be NOT NULL (used in JOIN conditions, no COALESCE wrapper), check for `not_null` test → `D4-TEST-001`, SAFE (generate test entry). For likely PK columns (used in MERGE ON clause or DISTINCT), check for `unique` test → `D4-TEST-002`, SAFE.
- **Description quality**: Empty model description → `D4-DOC-002`, SAFE (generate placeholder). Empty column descriptions → `D4-DOC-003`, SAFE.

If `schema_yml_content` is NOT available:
- Emit `D4-MISS-001`: "No schema.yml found — generate a complete schema.yml stub". Classify as SAFE. Generate the full stub with all columns from the final SELECT, with placeholder descriptions and recommended tests.

Check for unit test file:
- Look for `tests/unit/test_{model_name}.sql` or similar → `D4-UNIT-001` if missing, REVIEW (cannot auto-generate).

### Step A.6: Return findings

Return a JSON array of all findings:

```json
[
  {
    "rule_id": "D1-001",
    "dimension": "dialect_cleanup",
    "line_number": 42,
    "line_range": [42, 42],
    "description": "NVL() is Oracle-specific, use COALESCE()",
    "original": "NVL(col, 0)",
    "replacement": "COALESCE(col, 0)",
    "safety": "SAFE",
    "auto_fixable": true,
    "before_snippet": "SELECT NVL(amount, 0) AS amount",
    "after_snippet": "SELECT COALESCE(amount, 0) AS amount"
  }
]
```

## Output Specification

| Output | Format | Description |
|--------|--------|-------------|
| Findings array | JSON | All findings from D1-D4, each with rule_id, dimension, safety, before/after |
| Stats summary | JSON | Count of findings by dimension and safety category |

## Error Handling

### Unparseable SQL
If the model SQL cannot be parsed (e.g., deeply nested Jinja, dynamic SQL generation), report the parse error and skip the affected checks. Complete all checks that do not depend on SQL parsing.

### Ambiguous pattern match
If a pattern match is uncertain (e.g., a function name that could be either legacy or Snowflake), classify as REVIEW rather than SAFE.

### Missing inputs
If `schema_yml_content` or `dbt_project_yml_content` is not provided, skip the checks that depend on them and note the limitation in the output.
