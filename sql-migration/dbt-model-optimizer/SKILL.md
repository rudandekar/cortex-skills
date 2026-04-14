---
name: dbt-model-optimizer
version: 0.2.0
tier: project
author: rudandekar
created: 2026-04-07
last_updated: 2026-04-08
status: draft

description: >
  Optimizes a single dbt model SQL file across six dimensions: dialect cleanup,
  code quality, Snowflake-native patterns, testing/documentation, SQL performance,
  and cost optimization. Use this skill when the user asks to "optimize this dbt model",
  "clean up migrated dbt code", "review my dbt model", "check for Teradata/Oracle/SQL
  Server leftovers", "improve this dbt SQL", or mentions post-migration optimization,
  dbt code review, dialect cleanup, audit column review, or Snowflake best practices
  for a single model file. Also trigger when the user references a .sql file inside a
  dbt project and asks for quality improvement or performance tuning.
  Do NOT use for project-wide optimization across multiple models — use
  dbt-target-optimizer instead (future). Do NOT use for initial SQL conversion from
  legacy dialect — use SnowConvertLegacySQL instead. Do NOT use for Informatica XML
  extraction — use INFA2DBT instead.

compatibility:
  tools: [bash_tool, read_file, edit_file, write_file, snowflake_query]
  context: [SKILL-GUIDELINES.md]
---

# dbt Model Optimizer

Optimizes a single post-migration dbt model across six dimensions: (D1) dialect
cleanup, (D2) code quality and dbt conventions, (D3) Snowflake-native pattern
adoption, (D4) testing and documentation, (D5) SQL performance, and (D6) cost
optimization. Produces a categorized optimization report with SAFE/REVIEW/REQUIRES
APPROVAL classifications, applies safe changes automatically after user approval,
and validates all changes via dbt compile and test. Designed for models that have
already been converted from Informatica/legacy SQL to dbt but need post-migration
polish.

## Inputs

- **Required:** `model_path` — absolute or relative path to a single `.sql` dbt model file
- **Optional:** `source_dialect` — force dialect detection (`teradata`, `oracle`, `sqlserver`, `auto`). Default: `auto`
- **Optional:** `skip_live_profiling` — set `true` to skip D5/D6 even if Snowflake is connected. Default: `false`
- **Optional:** `auto_apply_safe` — set `true` to skip the HITL checkpoint and auto-apply all SAFE changes. Default: `false`
- **Optional:** `report_dir` — directory to write the optimization report file. Default: same directory as `model_path`. Set to a shared directory (e.g., `reports/optimization/`) when running across multiple models to collect all reports in one place.

## Pre-conditions

- The model file must exist and be readable
- For full validation (Step 9): a dbt project root with `dbt_project.yml` must exist as an ancestor of `model_path`
- For live profiling (Step 8): an active Snowflake connection must be available
- If pre-conditions for validation or profiling are not met, the skill degrades gracefully (static analysis only)

## Workflow

### Step 1: Input validation and environment detection

1. Read the model file at `model_path` in full. If the file does not exist or is empty, STOP and report the error.
2. Detect the dbt project root by walking up from `model_path` looking for `dbt_project.yml`. Record the path. If not found, note that validation (Step 9) will be limited to syntax checking only.
3. Look for a sibling `schema.yml` or `_schema.yml` file in the same directory as the model, or in a `schema/` subdirectory. Read it if found.
4. Probe Snowflake connection availability by attempting a lightweight query (`SELECT 1`). Record whether live profiling is available.
5. Detect source dialect from code patterns:
   - **Teradata**: `SEL `, `BT;`, `ET;`, `COLLECT STATS`, `.database.table`, `CASESPECIFIC`, `TITLE`, `COMPRESS`, `VOLATILE`
   - **Oracle**: `NVL(`, `DECODE(`, `SYSDATE`, `ROWNUM`, `(+)`, `DUAL`, `CONNECT BY`, `SUBSTR(`
   - **SQL Server**: `GETDATE()`, `ISNULL(`, `[brackets]`, `#temp`, `@@IDENTITY`, `SET NOCOUNT`, `TOP `
   - **Clean Snowflake**: None of the above detected
6. Log: model name, detected dialect, project root (or "none"), Snowflake connection (available/unavailable), schema.yml (found/not found).

### Step 2: Static analysis — Dialect cleanup (D1)

Load `references/dialect-patterns.md`. Scan the model for every pattern listed under the detected dialect section. For each match, emit a finding:

```json
{
  "rule_id": "D1-001",
  "dimension": "dialect_cleanup",
  "line_number": 42,
  "description": "NVL() is Oracle-specific, use COALESCE()",
  "original": "NVL(col, 0)",
  "replacement": "COALESCE(col, 0)",
  "safety": "SAFE",
  "auto_fixable": true
}
```

Also scan for:
- Residual Informatica variable references (`$$VAR`, `$PM`, `$$STGDB`)
- Hardcoded database/schema names that should use `var()` or `{{ target.schema }}`
- `CREATE VOLATILE TABLE` that should be a CTE
- Teradata `BT;`/`ET;` transaction markers (remove)
- `COLLECT STATISTICS` / `COLLECT STATS` (remove)

### Step 3: Static analysis — Code quality (D2)

Load `references/dbt-conventions.md`. Check each rule against the model:

**Config block checks:**
- `{{ config(...) }}` block present at top of file
- `materialized` is set and appropriate (table/view/incremental/ephemeral)
- `schema` is set
- `database=var('...')` present for models targeting specific databases
- `query_tag` present
- `on_schema_change` set for incremental models
- `incremental_strategy` set for incremental models

**CTE structure checks:**
- No unused CTEs (defined but not referenced in downstream CTEs or final SELECT)
- No redundant CTEs (two CTEs with identical SQL bodies)
- No inline subqueries in the final SELECT that should be CTEs
- CTE naming follows project conventions (snake_case, descriptive)

**Dependency checks:**
- No raw table references (e.g., `SCHEMA.TABLE`) — must use `{{ source('...', '...') }}` or `{{ ref('...') }}`
- Correct macro usage: N_/MT_/R_ prefix tables use `source()` for external tables; W_/WI_/ST_/EX_/EL_/SM_/GL_/FA_ prefix tables use `ref()`
- Incremental models use `{{ this }}` for self-references

**Hook checks:**
- If extracted JSON or comments indicate pre-SQL exists, `pre_hook` must be in config
- If post-SQL exists, `post_hook` must be in config
- Session-level SQL must be captured in hooks
- Hook SQL uses `{{ edw_convert_to_pst() }}` macro (not raw SQL)

**Audit column checks (per table prefix):**
- All target models (WI_, N_, MT_) use `EDWSF_*` naming (NOT `EDW_*`)
- N_ and MT_ models include `EDWSF_BATCH_ID`
- MT_ models use `CURRENT_TIMESTAMP(0)` for datetime audit columns and `CAST(CURRENT_USER() AS VARCHAR(30))` for user audit columns
- `edw_convert_to_pst()` is ONLY in hook SQL, NEVER in SELECT column expressions

### Step 4: Static analysis — Snowflake-native patterns (D3)

Load `references/snowflake-native.md`. Check for optimization opportunities:

- **QUALIFY adoption**: `ROW_NUMBER() OVER (...) AS rn` + `WHERE rn = 1` in outer query → collapse to `QUALIFY ROW_NUMBER() OVER (...) = 1`
- **FLATTEN for JSON**: Nested subqueries accessing `VARIANT` / `OBJECT` / `ARRAY` columns → `LATERAL FLATTEN()`
- **SPLIT_TO_TABLE**: String splitting via `STRTOK` in a loop or recursive CTE → `SPLIT_TO_TABLE()`
- **MATCH_RECOGNIZE**: Complex sessionization logic → consider `MATCH_RECOGNIZE`
- **Dynamic Table candidacy**: Model is a pure transform (no hooks, no pre/post SQL, no `{{ this }}` self-ref, no side effects) → candidate for `materialized='dynamic_table'` with appropriate `target_lag`
- **CASE to DECODE**: `CASE col WHEN 'a' THEN 1 WHEN 'b' THEN 2 END` with 4+ branches on same column → Snowflake `DECODE()` (optional style preference, mark as REVIEW)

Each finding includes before/after SQL snippets and safety classification.

### Step 5: Static analysis — Testing and documentation (D4)

Check the schema.yml file (found in Step 1) against the model:

- **Column coverage**: Every column in the final SELECT should be documented in schema.yml. Emit a generated stub for any missing column.
- **Test coverage**: `not_null` test for columns that appear to be NOT NULL (no COALESCE wrapping, used in JOINs). `unique` test for likely primary key columns.
- **Description quality**: Model-level description is non-empty and meaningful. Column descriptions are non-empty.
- **Unit test check**: Look for a corresponding test file in `tests/unit/` directory matching the model name.

For missing tests or documentation, generate the YAML stub that would fix the gap.

### Step 6: Generate categorized optimization report — HITL checkpoint

Aggregate all findings from Steps 2-5. Categorize each:

| Category | Criteria | Examples |
|----------|----------|----------|
| **SAFE** | Deterministic, non-semantic, auto-fixable | Dialect function replacements, unused CTE removal, naming fixes, test additions, audit column corrections |
| **REVIEW** | Likely correct but user should verify | QUALIFY rewrites, FLATTEN adoption, predicate pushdown, DECODE style changes |
| **REQUIRES APPROVAL** | Semantic-changing or architectural | CTE consolidation, materialization changes, Dynamic Table candidacy, clustering keys |

Produce the report in Markdown format with:
1. **Header**: model name, detected dialect, timestamp, Snowflake connection status
2. **Summary table**: counts by dimension and safety category
3. **SAFE changes**: full list with line numbers and before/after
4. **REVIEW items**: full list with rationale and before/after
5. **REQUIRES APPROVAL items**: full list with impact analysis

**Write the report to a persistent file:**
- File name: `optimization_report_{model_name}.md` (model name without `.sql` extension, lowercased)
- Location: `report_dir` if provided, otherwise the same directory as `model_path`
- If the file already exists, overwrite it (each run produces a fresh report)
- The report file is the authoritative record of what was found, applied, and recommended

### ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Approve SAFE changes

Present the summary table and SAFE change count to the user.

- If `auto_apply_safe` is `true`: skip this checkpoint, proceed to Step 7.
- Otherwise: Ask "Proceed with applying N SAFE changes? (y/n)"
- On approval: proceed to Step 7
- On rejection: ask which changes to exclude, then proceed
- On silence: wait — do not proceed

### Step 7: Apply SAFE changes

Apply all approved SAFE changes to the model file in this order:
1. Dialect cleanup (D1) — function replacements, syntax removals
2. Code quality (D2) — config block fixes, unused CTE removal, audit column corrections
3. Snowflake-native (D3) — only SAFE-classified items (none by default)
4. Testing/docs (D4) — schema.yml additions (additive only)

Track every change with before/after for potential rollback in Step 9. Do NOT modify REVIEW or REQUIRES APPROVAL items.

### Step 8: Live profiling — Performance (D5) and Cost (D6)

**Skip this step if:** Snowflake connection is unavailable OR `skip_live_profiling` is `true`. Note in the report: "Live profiling unavailable — connect to Snowflake for D5/D6 analysis."

If connected:

1. Run `dbt compile --select {model_name}` to get compiled SQL. If compile fails, report the error and skip this step.
2. Execute `EXPLAIN` on the compiled SQL. Capture the query plan.
3. Query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for recent executions matching the model's `query_tag` (last 30 days):
   - `BYTES_SPILLED_TO_LOCAL_STORAGE`, `BYTES_SPILLED_TO_REMOTE_STORAGE`
   - `PARTITIONS_SCANNED` vs `PARTITIONS_TOTAL`
   - `EXECUTION_TIME`, `COMPILATION_TIME`
   - `CREDITS_USED_CLOUD_SERVICES`
4. Delegate deep analysis to the `live-profiler` sub-agent. Load `agents/live-profiler/SKILL.md`.
5. Produce recommendations:
   - **Clustering key candidates**: date/timestamp columns first, then high-cardinality filter columns, max 4 keys
   - **Materialization audit**: table→view if <10 queries/day; table→incremental if updates are append-pattern
   - **Warehouse sizing**: upsize if bytes spilling >0; downsize if <20% utilization

All D5/D6 recommendations are classified as REQUIRES APPROVAL.

### Step 9: Full validation

1. If dbt project root was found:
   - Run `dbt compile --select {model_name}+` — verify Jinja compilation, ref/source resolution, SQL syntax
   - Run `dbt test --select {model_name}` — verify all tests pass (including any new tests added in Step 7)
2. If EXPLAIN was captured in Step 8, compare before/after query plans for scan reduction.
3. **On compile failure:** ROLLBACK all changes applied in Step 7, restore the original file, report the specific compilation error with line number.
4. **On test failure:** Report which test failed and likely cause. Do NOT rollback — test additions are additive and the failure may indicate a pre-existing data issue.
5. If no dbt project root: perform basic SQL syntax validation only (check for unclosed parentheses, unmatched quotes, etc.).

### Step 10: Final summary

**Update the report file** (written in Step 6) by appending the post-optimization sections:

6. **Changes applied**: list each change with before/after and line number
7. **Validation result**: compile pass/fail, test pass/fail, EXPLAIN comparison (if available)
8. **Remaining REVIEW items**: list with rationale — user should evaluate these next
9. **Remaining REQUIRES APPROVAL items**: list with impact analysis — user must decide
10. **D5/D6 recommendations** (if live profiling ran): clustering, materialization, warehouse sizing
11. **Next steps**: what the user should do after this optimization pass

The updated report file is the complete record of the optimization pass. Present an inline summary to the user containing:
- Path to the report file
- Count of changes applied vs remaining recommendations
- Validation status (pass/fail)
- Top 3 highest-impact remaining recommendations (if any)

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| Optimized model | Same as `model_path` (in-place edit) | SQL | The optimized dbt model |
| Optimization report file | `report_dir` or same dir as model | Markdown | `optimization_report_{model_name}.md` — full findings, changes, validation, recommendations |
| Inline summary | Conversation | Markdown | Condensed summary with report file path, counts, validation status |
| schema.yml additions | Same directory as model | YAML | New tests/columns (if D4 changes applied) |

The **report file** is the primary deliverable — it persists after the conversation ends and serves as the audit trail for the optimization. The inline summary is a convenience for the user during the session.

Always present the inline summary. If schema.yml was modified, show the diff.

## Error Handling

### Model file not found or empty
Report the exact path that was attempted. List files in the parent directory to help the user locate the correct file.

### No dbt project root found
Proceed with static analysis only (Steps 2-5). Skip validation (Step 9 limited to syntax checks). Warn that compile/test validation is unavailable.

### Snowflake connection unavailable
Skip Step 8 entirely. Note in report. All D1-D4 analysis proceeds normally.

### Compile failure after changes
Rollback ALL changes from Step 7. Restore the original file content. Report the specific error. Ask the user how to proceed.

### Partial parse/analysis failure
Complete all dimensions that succeed. Report each failed dimension with its error. Never suppress partial failures from the report.

### schema.yml not found
Skip D4 column coverage and test coverage checks. Note that test/doc analysis is limited. Generate a complete schema.yml stub as a recommendation.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Step 6 | 🟡 MEDIUM | Applying SAFE changes to model file |
| Step 8 | 🔴 HIGH | Any D5/D6 recommendations (clustering, materialization, warehouse) |

## Sub-Agents

| Agent | Skill Path | Scope | Requires Snowflake |
|-------|------------|-------|--------------------|
| Static Analyzer | `agents/static-analyzer/SKILL.md` | D1-D4 analysis | No |
| Live Profiler | `agents/live-profiler/SKILL.md` | D5-D6 analysis | Yes |
