---
name: sybase-to-snowflake
version: 0.3.0
tier: user
author: Deloitte FDE Practice
created: 2026-04-01
last_updated: 2026-04-02
status: active

description: >
  Converts Sybase ASE/IQ SQL scripts to Snowflake native SQL (DDL, DML, stored
  procedures, tasks, streams). Use this skill when the user uploads .sql files
  from Sybase, mentions "Sybase migration", "Sybase to Snowflake", "ASE conversion",
  "IQ migration", or asks to convert Sybase stored procedures, ETL scripts, or
  data warehouse SQL to Snowflake. Also trigger on Sybase-specific keywords:
  GETDATE, ISNULL, CONVERT, DATEPART, DATENAME, sysobjects, GO batch separator,
  RAISERROR, TINYINT, or linked server notation.
  Do NOT use for Informatica PowerCenter migrations — use infa2dbt-accelerator instead.
  Do NOT use for Talend or other ETL tool migrations.

compatibility:
  tools: [bash, read, write, edit, snowflake_sql_execute, task, glob, grep]
  context: [CLAUDE.md]
---

# Sybase-to-Snowflake SQL Converter

Converts Sybase ASE and IQ SQL scripts to Snowflake native SQL. This skill handles
the full migration lifecycle: parsing Sybase batch scripts, inventorying objects and
dependencies, classifying conversion complexity, generating Snowflake-native DDL/DML,
scoring conversion fidelity, and producing a test harness with synthetic data.

All type mappings, function translations, and pattern conversions are documented in
`references/sybase-to-snowflake-type-map.md`. Structured handoff contracts, TODO
block templates, and audit log formats are in `references/conversion-standards.md`.

## Guiding Principles

1. **Semantic equivalence over syntactic similarity.** The converted SQL must produce
   identical results to the Sybase source — not just compile. Every conversion is
   verified against reference output on seed data.
2. **Self-documenting output.** Every converted SQL file includes inline annotations
   tracing each non-trivial conversion back to its source construct and type-map rule.
   A reviewer reading the output should understand *what changed and why* without
   consulting the source.
3. **Structured handoffs.** Each workflow step produces a typed JSON artifact consumed
   by the next step. Contracts are defined in `references/conversion-standards.md`.
4. **Retry before quarantine.** Failed conversions get up to 2 targeted retries with
   structured diagnosis before being quarantined for manual review.
5. **Never drop silently.** Every Sybase construct must appear in the output — either
   converted, wrapped in a standardized TODO block, or logged as explicitly removed
   with rationale.
6. **Provenance in every file.** All generated files include a `meta` header block
   recording source file, converter version, timestamp, and applied rules.

## Security Hardening

- Use `snowflake_sql_execute` with `only_compile=true` for all validation — never
  execute converted DML against production schemas without explicit HITL approval
- Never embed credentials or connection strings in generated SQL files
- All seed data is synthetic — never copy production data into test artifacts
- Dollar-quote (`$$`) literals in Snowflake Scripting blocks must be properly escaped

## Inputs

- **Required:** `source_sql_path` — Path to a single `.sql` file or directory of `.sql` files
- **Optional:** `target_database` — Snowflake target database (default: active connection)
- **Optional:** `target_schema` — Snowflake target schema (default: `PUBLIC`)
- **Optional:** `output_dir` — Output directory (default: `./converted/`)
- **Optional:** `fidelity_threshold` — Minimum fidelity score to pass (default: `0.85`)
- **Optional:** `max_retries` — Max retry attempts per failed object (default: `2`)

## Pre-conditions

- Snowflake connection active with a warehouse for compilation checks
- Source `.sql` files readable and containing valid Sybase ASE or IQ SQL
- CREATE TABLE / CREATE VIEW / CREATE PROCEDURE privileges on target schema

## Invocation Modes

### Mode 1: Full Pipeline
```
Convert Sybase SQL in /path/to/scripts/ to Snowflake
```

### Mode 2: Step-Specific
```
Run sybase-to-snowflake Step 1-3 on /path/to/scripts/     (parse + inventory only)
Run sybase-to-snowflake Step 5 on /path/to/object_inventory.json  (generate only)
Run sybase-to-snowflake Step 7 on /path/to/converted/      (fidelity scoring only)
```

### Mode 3: Resume from Failure
```
Resume sybase-to-snowflake from Step 7 using /path/to/converted/logs/run_log.json
```

On resume: read `run_log.json`, skip steps with `status: passed`, re-execute from
the first step with `status: failed` or `status: pending`.

## Workflow

### Step 1: Ingest and parse source SQL files

Read all `.sql` files from `source_sql_path`. For each file:

1. Split on `GO` batch separators to isolate individual statement blocks
2. Classify each block: DDL, DML, procedural, or utility
3. Extract line number ranges for each block
4. Detect Sybase-specific constructs per block (list each one found)

Produce **`object_inventory.json`** (schema in `references/conversion-standards.md`):
each entry has block_id, classification, primary_object_name, line_range,
sybase_constructs_detected, source_file.

Present the inventory to the user as a markdown table.

### Step 2: Build dependency graph

From the inventory, extract all objects created and referenced. Build a directed
dependency graph showing data flow layers:
```
Source (external/linked) → Staging → Dimensions → Facts → Aggregation → Views
```

Flag: circular dependencies (STOP), orphan objects (warn), external references
(require configuration mapping).

Produce **`dependency_graph.json`** with nodes, edges, topological_order, warnings.

### Step 3: HITL CHECKPOINT — 🟡 MEDIUM — Approve inventory and dependency graph

Present: object count by type, dependency graph (ASCII or ordered list), warnings,
proposed conversion order (topological sort).

Required response: explicit approval. On rejection: ask what to correct.

### Step 4: Classify conversion complexity

Score each object using `references/complexity-scoring-rubric.md` across 7 dimensions.
Assign: **Simple** (0-3 pts), **Medium** (4-6 pts), **Complex** (7+ pts).

Produce **`complexity_report.json`** with per-object scores, tier assignment,
primary complexity drivers, and overall distribution summary.

Present: count per tier, top 5 most complex objects with drivers, risk areas.

### Step 5: Generate Snowflake native SQL

For each object in dependency order, generate Snowflake SQL:

1. Read `references/sybase-to-snowflake-type-map.md` for conversion rules
2. Apply conversions in order: remove artifacts → DDL type mapping → function
   translation → pattern replacement → SCD conversion → procedure wrapping →
   orchestration (full details in type-map reference)
3. **Annotate every non-trivial conversion** inline:
   ```sql
   -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP() (type-map §2: Date/Time Functions)
   -- [CONVERT] ISNULL(a,b) → COALESCE(a,b) (type-map §2: Null Handling)
   ```
4. **Insert standardized TODO blocks** for unsupported constructs (template in
   `references/conversion-standards.md`):
   ```sql
   -- ══════════════════════════════════════════════════════════════
   -- TODO: MANUAL CONVERSION REQUIRED
   -- Construct: <sybase_construct>
   -- Source: <file>:<line_range>
   -- Reason: No documented Snowflake equivalent
   -- Suggested approach: <best_effort_description>
   -- Review owner: Data Engineering Lead
   -- ══════════════════════════════════════════════════════════════
   ```
5. **Embed meta provenance** header in every output file:
   ```sql
   -- ================================================================
   -- Generated by: sybase-to-snowflake v0.3.0
   -- Source: <source_file>:<line_range>
   -- Object: <object_name> (<object_type>)
   -- Complexity: <tier> (score: <N>)
   -- Timestamp: <ISO-8601>
   -- Applied rules: <comma-separated list of type-map sections used>
   -- ================================================================
   ```
6. Write each object to `output_dir/` named by type and object
   (e.g., `ddl_dim_customer.sql`, `dml_load_fact_claims.sql`)
7. Generate `run_all.sql` master script in dependency order

Produce **`conversion_log.json`** per object: source_ref, target_file, rules_applied,
annotations_count, todo_blocks_count, meta_header.

### Step 6: HITL CHECKPOINT — 🔴 HIGH — Review converted SQL

Present by complexity tier:
- **Simple:** count and names — bulk approval
- **Medium:** converted SQL with key translation decisions highlighted
- **Complex:** side-by-side (Sybase → Snowflake) with annotations for every
  non-trivial conversion

Required response: explicit approval per tier. On rejection: identify objects
to revise, apply corrections, re-present.

### Step 7: Score conversion fidelity

For each converted object with testable DML logic (not pure DDL):

1. Generate synthetic seed data (50-200 rows per source table) with explicit
   edge cases: NULL rows, duplicate key rows, boundary values, empty strings,
   negative amounts, future dates
2. Generate reference SQL — a naive direct translation of the Sybase logic
   (correct output, not optimized, not Snowflake-idiomatic)
3. Execute both converted SQL and reference SQL against seed data
4. Compare on 5 weighted dimensions:

   | Component | Weight | Pass Criteria |
   |-----------|--------|---------------|
   | Row count | 30% | Exact match |
   | Column match | 15% | All columns present (case-insensitive) |
   | NULL profile | 15% | NULL rates within ±2% per column |
   | Aggregate match | 25% | SUM/COUNT/MAX/MIN exact match |
   | Spot check | 15% | 10 sampled rows match exactly |

5. Calculate weighted fidelity score (0.0 – 1.0)

**Routing decision:**
- **PASS** (score >= `fidelity_threshold`): proceed to Step 8
- **RETRY** (score < threshold AND `retry_count < max_retries`): generate
  structured diagnosis (failing components, deltas, likely cause, suggested fix),
  return to Step 5 for targeted correction only — do not regenerate from scratch
- **QUARANTINE** (retries exhausted): move to `output_dir/quarantine/` with
  TODO block, final score, and diagnosis. Log in `quarantine_inventory.json`.

Produce **`fidelity_scores.json`** per object: overall_score, component_scores,
threshold_applied, retry_count, decision, diagnosis (if failed).

### Step 8: HITL CHECKPOINT — 🟡 MEDIUM — Approve fidelity results

Present: total objects scored, pass/retry/quarantine counts, average fidelity,
quarantine inventory (if any).

If quarantined objects exist: ask whether to proceed with passing objects only,
or pause for manual remediation.

Required response: explicit decision.

### Step 9: Generate test harness

For each converted object:

1. **Seed data** (from Step 7, or generate fresh for DDL-only objects)
2. **DDL test:** execute CREATE statements, load seed data via INSERT
3. **DML test:** execute converted DML against seeded tables, verify row counts,
   key column population, aggregate reasonableness
4. **Reconciliation queries:** row count, SUM/COUNT of numerics, DISTINCT count
   of categoricals, NULL profile comparison

Write to `output_dir/tests/`: `seed_data.sql`, `test_ddl.sql`, `test_dml.sql`,
`reconciliation_queries.sql`

### Step 10: HITL CHECKPOINT — 🟡 MEDIUM — Approve test plan

Present: seed table count, row counts, validation checks, reconciliation queries.
Required response: explicit approval before executing tests.
On approval: execute test suite, present results.

### Step 11: Generate conversion report and finalize

Produce `conversion_report.json`:
1. **Executive summary:** objects converted, pass rate, complexity distribution,
   average fidelity score
2. **Object inventory:** every object with source, complexity, fidelity, status
3. **Conversion decisions log:** every non-trivial translation with type-map reference
4. **Quarantine inventory:** failed objects with scores, diagnosis, remediation notes
5. **Test results:** seed coverage, DML outcomes, reconciliation findings
6. **Known limitations:** constructs that could not be automatically converted

Produce **`run_log.json`** — complete pipeline state for resume-from-failure:
per-step status (passed/failed/pending), timestamps, file paths, error messages.

### Step 12: Generate Snowflake optimization recommendations

Analyze all converted SQL against Snowflake-native optimization rules defined in
`references/optimization-rules.md`. For each object, evaluate 8 optimization categories:

1. **Clustering keys** — Recommend `CLUSTER BY` for fact tables on date dimension keys
   and any table with predictable range-filter or join patterns. Prioritize low-to-high
   cardinality column order. Skip tables under ~1TB (note as conditional).
2. **Transient tables** — Recommend `CREATE TRANSIENT TABLE` for staging tables that are
   truncated and reloaded each batch (no Fail-safe needed, reduces storage cost).
3. **Search Optimization** — Identify high-cardinality lookup columns used in point
   queries (policy_number, claim_number, customer_id). Recommend `ADD SEARCH OPTIMIZATION`
   on specific columns. Requires Enterprise Edition.
4. **Query Acceleration** — Flag warehouses running ad-hoc analytics on fact/aggregate
   tables as QAS candidates. Recommend `ENABLE_QUERY_ACCELERATION = TRUE`.
5. **Informational constraints** — Generate `ALTER TABLE ... ADD CONSTRAINT` DDL for all
   PK/FK relationships identified in the dependency graph. These are informational (not
   enforced) but enable BI tool join inference and Snowflake join elimination.
6. **Dynamic Table candidates** — Analyze aggregate/mart tables. If the DML is a single
   `INSERT ... SELECT` with no procedural logic, flag as a Dynamic Table candidate.
   Generate sample `CREATE DYNAMIC TABLE` DDL with recommended `TARGET_LAG`.
7. **Streams + Tasks roadmap** — For batch ETL patterns (staging → dim → fact), recommend
   a modernization path using Streams for CDC and Tasks for scheduling. Output as a
   roadmap (not auto-applied DDL) since this is an architectural decision.
8. **Storage tuning** — Recommend `DATA_RETENTION_TIME_IN_DAYS` per table type:
   staging=1, dimensions=7-14, facts=30-90, audit=90. Flag any tables where the default
   (1 day) may be insufficient for operational recovery.

For each recommendation, produce:
- **Category** and **rule ID** (O1–O8)
- **Severity**: HIGH (strong ROI, low risk) / MEDIUM (conditional on workload) / LOW (nice-to-have)
- **Rationale** citing the specific Snowflake documentation section
- **Exact DDL/SQL** to apply
- **Classification**: `auto-apply` (safe, additive, no schema change) vs `manual-review`
  (requires architecture decision or Enterprise Edition)

Produce **`optimization_report.json`** (schema in `references/conversion-standards.md`):
per-object recommendations with category, severity, DDL, and classification.

Produce **`optimization_ddl.sql`** — ready-to-execute DDL grouped by category, with
inline comments explaining each recommendation. Auto-apply items first, then
manual-review items commented out with `-- [MANUAL REVIEW]` prefix.

### Step 12a: HITL CHECKPOINT — 🟡 MEDIUM — Approve optimization recommendations

Present: recommendation count by category and severity, auto-apply vs manual-review
split, estimated impact summary.

For auto-apply recommendations: ask whether to apply them to the converted DDL files
(amending `CREATE TABLE` statements) or keep them as a separate `optimization_ddl.sql`.

For manual-review recommendations: present as advisory only — no changes applied
without explicit per-item approval.

Required response: explicit approval of auto-apply scope. On rejection: ask which
categories to exclude.

### Output Summary Template

```
═══════════════════════════════════════════════════════════════
SYBASE-TO-SNOWFLAKE CONVERSION COMPLETE
═══════════════════════════════════════════════════════════════
Source files processed: {N}
Statement blocks parsed: {N}
Objects identified: {N} ({N} tables, {N} views, {N} procs, {N} other)

Complexity distribution:
  Simple:  {N} objects
  Medium:  {N} objects
  Complex: {N} objects

Conversion results:
  Passed:      {N} (avg fidelity: {score})
  Retried:     {N} (succeeded after retry)
  Quarantined: {N} (manual review required)

Fidelity scores:
  Average: {score}  |  Min: {score}  |  Max: {score}

Test harness:
  Seed tables: {N}  |  Checks: {N}  |  Passed: {N}  |  Failed: {N}

Optimization recommendations:
  Total: {N}  |  Auto-apply: {N}  |  Manual-review: {N}
  Categories: Clustering({N}) Transient({N}) Search({N}) QAS({N})
              Constraints({N}) DynTable({N}) Streams({N}) Storage({N})

Output written to: {output_dir}
Run log: {output_dir}/logs/run_log.json
═══════════════════════════════════════════════════════════════
```

## Output Specification

### Files produced

| File | Location | Format | Description |
|------|----------|--------|-------------|
| `ddl_*.sql` | `output_dir/` | SQL | One file per table/view DDL |
| `dml_*.sql` | `output_dir/` | SQL | One file per DML operation |
| `proc_*.sql` | `output_dir/` | SQL | One file per stored procedure |
| `run_all.sql` | `output_dir/` | SQL | Master execution script (dependency order) |
| `seed_data.sql` | `output_dir/tests/` | SQL | Synthetic test data |
| `test_ddl.sql` | `output_dir/tests/` | SQL | DDL validation script |
| `test_dml.sql` | `output_dir/tests/` | SQL | DML validation script |
| `reconciliation_queries.sql` | `output_dir/tests/` | SQL | Source-to-target reconciliation |
| `conversion_report.json` | `output_dir/logs/` | JSON | Full conversion summary |
| `object_inventory.json` | `output_dir/logs/` | JSON | Step 1 output |
| `dependency_graph.json` | `output_dir/logs/` | JSON | Step 2 output |
| `complexity_report.json` | `output_dir/logs/` | JSON | Step 4 output |
| `conversion_log.json` | `output_dir/logs/` | JSON | Step 5 per-object log |
| `fidelity_scores.json` | `output_dir/logs/` | JSON | Step 7 per-object scores |
| `quarantine_inventory.json` | `output_dir/logs/` | JSON | Quarantined objects |
| `run_log.json` | `output_dir/logs/` | JSON | Pipeline state for resume |
| `optimization_report.json` | `output_dir/logs/` | JSON | Step 12 recommendations |
| `optimization_ddl.sql` | `output_dir/` | SQL | Ready-to-execute optimization DDL |

## Error Handling

### Empty or unreadable SQL files
List all files. Report unreadable ones. Continue with valid files. No valid files = STOP.

### Unsupported Sybase constructs
Insert standardized TODO block (template in `references/conversion-standards.md`).
Never silently drop — every construct must appear in output or be logged.

### Compilation failures in validation
Attempt auto-fix (max 2 attempts). If still failing: route to fidelity scoring
which will handle retry/quarantine routing.

### Fidelity score below threshold
Retry with structured diagnosis (max `max_retries`). On exhaustion: quarantine
with full diagnosis. Never suppress — every object has a terminal status.

### Circular dependencies
STOP. Report cycle path. Ask user how to break the cycle.

### Partial success
Complete all convertible objects. Report each failure in `quarantine_inventory.json`
and `conversion_report.json`. Never suppress partial failures.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|----------------------|
| Step 3 | 🟡 MEDIUM | Object inventory and dependency graph |
| Step 6 | 🔴 HIGH | Converted SQL review (per complexity tier) |
| Step 8 | 🟡 MEDIUM | Fidelity results and quarantine handling |
| Step 10 | 🟡 MEDIUM | Test plan before execution |
| Step 12 | 🟡 MEDIUM | Optimization recommendations (auto-apply scope) |

## Per-Step Audit Logging

Every step writes a structured JSON log to `output_dir/logs/`. Each log entry includes:
`step_id`, `step_name`, `status` (passed/failed/skipped), `started_at`, `completed_at`,
`objects_processed`, `errors` (array), `artifacts_produced` (array of file paths).

The `run_log.json` aggregates all step logs for pipeline-level state and
resume-from-failure support.

## Reference Files

- `references/sybase-to-snowflake-type-map.md` — Data types, functions, patterns, system objects, control flow, format codes
- `references/complexity-scoring-rubric.md` — 7-dimension weighted scoring with quarantine routing
- `references/conversion-standards.md` — Handoff JSON schemas, TODO block template, audit log format, meta header template, output summary template
- `references/optimization-rules.md` — 8 optimization categories with detection logic, DDL templates, severity classification

## Example Invocation

```
User: Convert the Sybase ETL scripts in /path/to/sybase/ to Snowflake SQL

Converter:
 1. Read 5 .sql files, split into 47 blocks → object_inventory.json
 2. Build dependency graph: staging → dims → facts → aggs → views
 3. ⚠️ Present inventory for approval
 4. Classify: 12 Simple, 7 Medium, 4 Complex → complexity_report.json
 5. Convert 23 objects with annotations + meta headers → conversion_log.json
 6. ⚠️ Present converted SQL (bulk Simple, detailed Complex)
 7. Score fidelity: 19 PASS (avg 0.91), 2 RETRY→PASS, 2 QUARANTINE
 8. ⚠️ Present fidelity results + quarantine inventory
 9. Generate test harness: 6 seed tables, 15 checks
10. ⚠️ Present test plan for approval
11. Execute tests, generate conversion_report.json + run_log.json
12. Analyze for optimizations: 14 recommendations (8 auto-apply, 6 manual)
13. ⚠️ Present optimization recommendations for approval
```
