# Conversion Standards

> Reference for the `sybase-to-snowflake` skill v0.3.0.
> Defines structured handoff JSON schemas, TODO block templates, audit log formats,
> and meta provenance headers used across all workflow steps.

---

## Table of Contents

1. [Handoff JSON Schemas](#1-handoff-json-schemas)
2. [TODO Block Template](#2-todo-block-template)
3. [Meta Provenance Header](#3-meta-provenance-header)
4. [Conversion Annotation Format](#4-conversion-annotation-format)
5. [Audit Log Format](#5-audit-log-format)
6. [Fidelity Scoring Contract](#6-fidelity-scoring-contract)
7. [Quarantine Entry Format](#7-quarantine-entry-format)
8. [Run Log Schema](#8-run-log-schema)
9. [Optimization Report Schema](#9-optimization-report-schema)

---

## 1. Handoff JSON Schemas

Each workflow step produces a typed JSON artifact consumed by downstream steps.
All JSON files are written to `output_dir/logs/`.

### 1.1 object_inventory.json (Step 1 → Steps 2, 4, 5)

```json
{
  "version": "0.3.0",
  "source_path": "/path/to/source/",
  "generated_at": "2026-04-01T12:00:00Z",
  "file_count": 5,
  "block_count": 47,
  "blocks": [
    {
      "block_id": "sql1_001",
      "source_file": "sql1.sql",
      "line_range": [1, 42],
      "classification": "DDL",
      "primary_object_name": "stg_policies",
      "secondary_objects": [],
      "sybase_constructs_detected": [
        "IF_EXISTS_DROP",
        "GO",
        "GETDATE",
        "CONVERT_WITH_STYLE",
        "LINKED_SERVER"
      ],
      "raw_sql_hash": "sha256:abc123..."
    }
  ]
}
```

**Required fields per block:**
- `block_id` — Unique identifier: `{file_stem}_{3-digit sequence}`
- `source_file` — Filename (not full path)
- `line_range` — `[start_line, end_line]` (1-indexed, inclusive)
- `classification` — One of: `DDL`, `DML`, `PROCEDURAL`, `UTILITY`
- `primary_object_name` — The object created or primarily operated on
- `secondary_objects` — Other objects referenced (for dependency extraction)
- `sybase_constructs_detected` — Array of construct identifiers from the canonical list
- `raw_sql_hash` — SHA-256 of the raw source block for change detection

### 1.2 dependency_graph.json (Step 2 → Steps 3, 5)

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:00:00Z",
  "nodes": [
    {
      "object_name": "stg_policies",
      "object_type": "TABLE",
      "layer": "staging",
      "source_block_id": "sql1_001"
    }
  ],
  "edges": [
    {
      "from": "stg_policies",
      "to": "fact_policy",
      "relationship": "data_flow"
    }
  ],
  "topological_order": [
    "stg_policies",
    "stg_customers",
    "dim_customer",
    "fact_policy"
  ],
  "warnings": [
    {
      "type": "EXTERNAL_REFERENCE",
      "object": "OperationalDB..dbo.policies",
      "message": "Linked server reference requires configuration mapping"
    }
  ],
  "layers": {
    "external": ["OperationalDB..dbo.policies"],
    "staging": ["stg_policies", "stg_customers"],
    "dimension": ["dim_customer", "dim_product"],
    "fact": ["fact_policy", "fact_claims"],
    "aggregation": ["agg_policy_monthly"],
    "view": ["v_policy_detail"]
  }
}
```

**Warning types:** `CIRCULAR_DEPENDENCY` (blocking), `ORPHAN_OBJECT`, `EXTERNAL_REFERENCE`,
`SELF_REFERENCE`, `MISSING_DEPENDENCY`.

### 1.3 complexity_report.json (Step 4 → Steps 5, 6)

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:00:00Z",
  "rubric_version": "complexity-scoring-rubric.md v0.3.0",
  "summary": {
    "total_objects": 23,
    "simple": 12,
    "medium": 7,
    "complex": 4
  },
  "objects": [
    {
      "object_name": "dim_customer",
      "block_id": "sql2_004",
      "scores": {
        "line_count": 2,
        "join_complexity": 1,
        "scd_pattern": 3,
        "procedural_depth": 1,
        "case_depth": 2,
        "derived_measures": 2,
        "sybase_construct_count": 2
      },
      "total_score": 13,
      "tier": "Complex",
      "primary_drivers": ["SCD Type 2", "Nested CASE expressions", "Many derived measures"]
    }
  ]
}
```

### 1.4 conversion_log.json (Step 5 → Steps 6, 7)

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:00:00Z",
  "objects": [
    {
      "object_name": "dim_customer",
      "block_id": "sql2_004",
      "source_file": "sql2.sql",
      "source_line_range": [85, 205],
      "target_file": "ddl_dim_customer.sql",
      "tier": "Complex",
      "rules_applied": ["§1:DATETIME→TIMESTAMP_NTZ", "§2:GETDATE→CURRENT_TIMESTAMP", "§2:ISNULL→COALESCE", "§2:DATEPART→DATE_PART", "§2:DATEDIFF→DATEDIFF", "§3:SCD2_EXPIRE_INSERT", "§3:IF_EXISTS→CREATE_OR_REPLACE"],
      "annotations_count": 14,
      "todo_blocks_count": 0,
      "meta_header_present": true,
      "conversion_notes": "SCD2 expire+insert converted to 3-step pattern"
    }
  ]
}
```

### 1.5 fidelity_scores.json (Step 7 → Step 8)

See [Section 6: Fidelity Scoring Contract](#6-fidelity-scoring-contract).

### 1.6 quarantine_inventory.json (Step 7 → Steps 8, 11)

See [Section 7: Quarantine Entry Format](#7-quarantine-entry-format).

### 1.7 run_log.json (Step 11 → Resume)

See [Section 8: Run Log Schema](#8-run-log-schema).

---

## 2. TODO Block Template

Insert this block for every Sybase construct that has no documented Snowflake equivalent
or requires manual intervention. Every construct in the source must appear in the output —
either converted or wrapped in a TODO block.

```sql
-- ══════════════════════════════════════════════════════════════
-- TODO: MANUAL CONVERSION REQUIRED
-- Construct: <sybase_construct_name>
-- Source: <source_file>:<start_line>-<end_line>
-- Block ID: <block_id>
-- Reason: <why automatic conversion is not possible>
-- Suggested approach: <best_effort_description_of_how_to_convert>
-- Review owner: Data Engineering Lead
-- ══════════════════════════════════════════════════════════════
```

**Rules:**
- The `Construct` field uses the canonical name from the type-map (e.g., `CURSOR`, `DYNAMIC_SQL`, `RAISERROR_CUSTOM`)
- The `Source` field uses the format `filename:start_line-end_line`
- The `Block ID` matches the `block_id` in `object_inventory.json`
- The `Suggested approach` must never be empty — provide at least a directional recommendation
- The `Review owner` defaults to `Data Engineering Lead` but can be overridden by the user
- TODO blocks are counted in `conversion_log.json` → `todo_blocks_count`
- TODO blocks are included in the `quarantine_inventory.json` if the parent object is quarantined

---

## 3. Meta Provenance Header

Embed at the top of every generated SQL file. This header enables traceability from
any output file back to its source, converter version, and applied rules.

```sql
-- ================================================================
-- Generated by: sybase-to-snowflake v0.3.0
-- Source: <source_file>:<start_line>-<end_line>
-- Object: <object_name> (<object_type>)
-- Block ID: <block_id>
-- Complexity: <tier> (score: <total_score>)
-- Timestamp: <ISO-8601 UTC>
-- Applied rules: <comma-separated list of type-map section references>
-- ================================================================
```

**Field definitions:**
- `Source` — Filename and line range in the original Sybase script
- `Object` — The primary object name and type (TABLE, VIEW, PROCEDURE, DML)
- `Block ID` — Cross-reference to `object_inventory.json`
- `Complexity` — Tier and numeric score from `complexity_report.json`
- `Timestamp` — When the conversion was generated (UTC ISO-8601)
- `Applied rules` — Section references from `sybase-to-snowflake-type-map.md` (e.g., `§1:DATETIME→TIMESTAMP_NTZ, §2:GETDATE→CURRENT_TIMESTAMP`)

---

## 4. Conversion Annotation Format

Inline annotations mark every non-trivial conversion within the generated SQL.
A conversion is "non-trivial" if it changes function names, alters syntax structure,
or remaps data types — not for cosmetic changes like removing `GO` or `SET NOCOUNT ON`.

**Format:**
```sql
-- [CONVERT] <source_construct> → <target_construct> (type-map <section_ref>)
```

**Examples:**
```sql
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP() (type-map §2: Date/Time Functions)
-- [CONVERT] ISNULL(a,b) → COALESCE(a,b) (type-map §2: Null Handling)
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) (type-map §2: CONVERT Function Mapping)
-- [CONVERT] DATETIME → TIMESTAMP_NTZ (type-map §1: Date/Time Types)
-- [CONVERT] IF EXISTS/DROP/CREATE → CREATE OR REPLACE (type-map §3: IF EXISTS Pattern)
-- [CONVERT] SCD2 UPDATE+INSERT → 3-step expire+insert pattern (type-map §3: SCD Type 2)
-- [REMOVE] SET NOCOUNT ON — No Snowflake equivalent needed (type-map §5: Batch and Control Flow)
-- [REMOVE] GO — Snowflake uses semicolons (type-map §5: Batch and Control Flow)
```

**Annotation prefixes:**
- `[CONVERT]` — A construct was translated to a different Snowflake equivalent
- `[REMOVE]` — A construct was intentionally removed with documented rationale
- `[TODO]` — Links to a TODO block (annotation placed before the block)

**Rules:**
- Place the annotation on the line immediately before the converted code
- One annotation per conversion — do not combine multiple conversions into one annotation
- The `type-map` reference must match an actual section in `sybase-to-snowflake-type-map.md`
- Count all annotations per file and report in `conversion_log.json` → `annotations_count`

---

## 5. Audit Log Format

Every workflow step writes a structured JSON log entry. All step logs are aggregated
into `run_log.json` (see Section 8).

### Step Log Entry

```json
{
  "step_id": 1,
  "step_name": "ingest_and_parse",
  "status": "passed",
  "started_at": "2026-04-01T12:00:00Z",
  "completed_at": "2026-04-01T12:00:15Z",
  "duration_seconds": 15,
  "objects_processed": 47,
  "errors": [],
  "warnings": [
    "sql3.sql: Line 142 contains non-UTF8 characters — replaced with '?'"
  ],
  "artifacts_produced": [
    "output_dir/logs/object_inventory.json"
  ],
  "hitl_checkpoint": false
}
```

**Required fields:**
- `step_id` — Integer (1-12) matching the workflow step number
- `step_name` — Canonical step name (see table below)
- `status` — One of: `passed`, `failed`, `skipped`, `pending`, `awaiting_approval`
- `started_at` / `completed_at` — ISO-8601 UTC timestamps
- `duration_seconds` — Wall-clock time for the step
- `objects_processed` — Count of objects handled in this step
- `errors` — Array of error strings (empty if none)
- `warnings` — Array of warning strings (empty if none)
- `artifacts_produced` — Array of file paths created by this step
- `hitl_checkpoint` — Boolean indicating whether this step requires human approval

### Canonical Step Names

| Step ID | Step Name | HITL |
|---------|-----------|------|
| 1 | `ingest_and_parse` | No |
| 2 | `build_dependency_graph` | No |
| 3 | `approve_inventory` | Yes |
| 4 | `classify_complexity` | No |
| 5 | `generate_snowflake_sql` | No |
| 6 | `review_converted_sql` | Yes |
| 7 | `score_fidelity` | No |
| 8 | `approve_fidelity` | Yes |
| 9 | `generate_test_harness` | No |
| 10 | `approve_test_plan` | Yes |
| 11 | `finalize_report` | No |
| 12 | `generate_optimization_recommendations` | No |
| 12a | `approve_optimizations` | Yes |

---

## 6. Fidelity Scoring Contract

### fidelity_scores.json

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:00:00Z",
  "threshold": 0.85,
  "max_retries": 2,
  "summary": {
    "total_scored": 15,
    "passed": 12,
    "retried_and_passed": 2,
    "quarantined": 1,
    "average_score": 0.91,
    "min_score": 0.62,
    "max_score": 0.98
  },
  "scores": [
    {
      "object_name": "dim_customer",
      "block_id": "sql2_004",
      "target_file": "ddl_dim_customer.sql",
      "overall_score": 0.92,
      "component_scores": {
        "row_count": { "weight": 0.30, "score": 1.0, "detail": "150/150 rows match" },
        "column_match": { "weight": 0.15, "score": 1.0, "detail": "12/12 columns present" },
        "null_profile": { "weight": 0.15, "score": 0.87, "detail": "credit_score: 3.2% delta (threshold 2%)" },
        "aggregate_match": { "weight": 0.25, "score": 0.88, "detail": "SUM(premium) off by 0.01 — rounding difference" },
        "spot_check": { "weight": 0.15, "score": 0.90, "detail": "9/10 sampled rows match" }
      },
      "threshold_applied": 0.85,
      "retry_count": 0,
      "decision": "PASS",
      "diagnosis": null
    }
  ]
}
```

### Routing Decision Logic

```
IF overall_score >= threshold:
    decision = "PASS"
ELIF retry_count < max_retries:
    decision = "RETRY"
    Generate diagnosis (see below)
    Increment retry_count
    Return to Step 5 for targeted correction
ELSE:
    decision = "QUARANTINE"
    Move file to output_dir/quarantine/
    Write entry to quarantine_inventory.json
```

### Diagnosis Object (for RETRY and QUARANTINE)

```json
{
  "failing_components": ["null_profile", "spot_check"],
  "deltas": {
    "null_profile": { "column": "credit_score", "expected_null_pct": 5.0, "actual_null_pct": 8.2 },
    "spot_check": { "row_index": 7, "column": "age_band", "expected": "35-44", "actual": "25-34" }
  },
  "likely_cause": "DATEDIFF age calculation using wrong date part — YEAR vs full date difference",
  "suggested_fix": "Replace DATEDIFF('YEAR', dob, CURRENT_DATE()) with FLOOR(DATEDIFF('DAY', dob, CURRENT_DATE()) / 365.25) for accurate age",
  "type_map_reference": "§2: Date/Time Functions — DATEDIFF"
}
```

**Rules:**
- Diagnosis is required for every RETRY and QUARANTINE decision
- `likely_cause` must be specific — not generic "conversion error"
- `suggested_fix` must reference a type-map section when applicable
- On RETRY: only the failing components are re-addressed — do not regenerate the entire file

---

## 7. Quarantine Entry Format

### quarantine_inventory.json

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:00:00Z",
  "quarantine_count": 2,
  "entries": [
    {
      "object_name": "dq_audit_procedure",
      "block_id": "sql4_001",
      "source_file": "sql4.sql",
      "source_line_range": [1, 180],
      "quarantine_file": "quarantine/proc_dq_audit_procedure.sql",
      "tier": "Complex",
      "final_fidelity_score": 0.62,
      "retry_count": 2,
      "final_diagnosis": {
        "failing_components": ["aggregate_match", "spot_check"],
        "likely_cause": "Procedural branching in DQ checks produces different execution paths under edge case seed data",
        "suggested_fix": "Manual review of IF/ELSE branch logic in lines 45-120; verify Snowflake Scripting variable scoping"
      },
      "todo_blocks": [
        {
          "construct": "RAISERROR_CUSTOM",
          "source_line_range": [155, 158],
          "reason": "Custom severity levels not supported in Snowflake RAISE"
        }
      ],
      "remediation_notes": "Assign to senior developer for manual Snowflake Scripting conversion. Estimated 2-4 hours of manual work.",
      "quarantined_at": "2026-04-01T12:15:00Z"
    }
  ]
}
```

**Rules:**
- Every quarantined object gets a file in `output_dir/quarantine/` with the partially-converted SQL,
  a meta header, and TODO blocks for unresolved constructs
- The `final_diagnosis` is the diagnosis from the last retry attempt
- `remediation_notes` should provide actionable guidance for the manual reviewer
- Quarantined files retain all successful conversions — only the failing portions are flagged

---

## 8. Run Log Schema

The `run_log.json` aggregates all step-level audit logs into a single pipeline state
document. This is the primary input for **Mode 3: Resume from Failure**.

```json
{
  "version": "0.3.0",
  "pipeline_id": "run_20260401_120000",
  "started_at": "2026-04-01T12:00:00Z",
  "completed_at": "2026-04-01T12:20:00Z",
  "overall_status": "completed_with_quarantine",
  "source_path": "/path/to/source/",
  "output_dir": "/path/to/converted/",
  "parameters": {
    "target_database": "ANALYTICS_DB",
    "target_schema": "PUBLIC",
    "fidelity_threshold": 0.85,
    "max_retries": 2
  },
  "steps": [
    {
      "step_id": 1,
      "step_name": "ingest_and_parse",
      "status": "passed",
      "started_at": "2026-04-01T12:00:00Z",
      "completed_at": "2026-04-01T12:00:15Z",
      "duration_seconds": 15,
      "objects_processed": 47,
      "errors": [],
      "warnings": [],
      "artifacts_produced": ["output_dir/logs/object_inventory.json"],
      "hitl_checkpoint": false
    }
  ],
  "summary": {
    "total_objects": 23,
    "passed": 20,
    "retried_and_passed": 2,
    "quarantined": 1,
    "average_fidelity": 0.91,
    "complexity_distribution": { "simple": 12, "medium": 7, "complex": 4 }
  }
}
```

**`overall_status` values:**
- `completed` — All objects passed fidelity scoring and tests
- `completed_with_quarantine` — Some objects quarantined but pipeline finished
- `failed` — Pipeline stopped at an error (check `steps` for the failing step)
- `awaiting_approval` — Pipeline paused at a HITL checkpoint
- `in_progress` — Pipeline currently running (should not appear in a finished log)

**Resume logic:**
1. Read `run_log.json`
2. Find the first step where `status` is `failed` or `pending`
3. Re-execute from that step, reusing artifacts from all prior `passed` steps
4. If resuming from Step 7 (fidelity): only re-score objects that were `RETRY` or `QUARANTINE`

---

## 9. Optimization Report Schema

### 9.1 optimization_report.json (Step 12 → Step 12a HITL)

Counts and values below are illustrative — actual numbers depend on the source workload.

```json
{
  "version": "0.3.0",
  "generated_at": "2026-04-01T12:25:00Z",
  "source_pipeline": "run_20260401_120000",
  "total_recommendations": 14,
  "by_classification": {
    "auto_apply": 8,
    "manual_review": 6
  },
  "by_severity": {
    "HIGH": 5,
    "MEDIUM": 6,
    "LOW": 3
  },
  "by_category": {
    "O1_clustering_keys": 3,
    "O2_transient_tables": 2,
    "O3_search_optimization": 1,
    "O4_query_acceleration": 1,
    "O5_informational_constraints": 4,
    "O6_dynamic_table_candidates": 1,
    "O7_streams_tasks_roadmap": 1,
    "O8_storage_tuning": 1
  },
  "recommendations": [
    {
      "rec_id": "R001",
      "rule_id": "O1",
      "category": "clustering_keys",
      "target_object": "fact_policy",
      "severity": "HIGH",
      "classification": "auto-apply",
      "rationale": "Fact table with date dimension key (date_key) and downstream range filters in v_policy_detail",
      "ddl": "ALTER TABLE {schema}.fact_policy CLUSTER BY (date_key, product_key);",
      "doc_ref": "https://docs.snowflake.com/en/user-guide/tables-clustering-keys",
      "conditions": "Benefit increases with table size; most impactful above ~1TB"
    }
  ]
}
```

**Required fields per recommendation:**
- `rec_id` — Sequential identifier: `R{3-digit sequence}`
- `rule_id` — Rule from optimization-rules.md: `O1`–`O8`
- `category` — Human-readable category name
- `target_object` — Table or warehouse name
- `severity` — `HIGH`, `MEDIUM`, or `LOW`
- `classification` — `auto-apply` or `manual-review`
- `rationale` — Why this recommendation applies to this specific object
- `ddl` — Exact SQL to execute (with `{schema}` placeholder)
- `doc_ref` — Snowflake documentation URL
- `conditions` — Any prerequisites or caveats (empty string if none)

### 9.2 optimization_ddl.sql (Step 12 output)

Structure:
```sql
-- ═══════════════════════════════════════════════════════════════
-- SNOWFLAKE OPTIMIZATION DDL
-- Generated by sybase-to-snowflake converter, Step 12
-- ═══════════════════════════════════════════════════════════════

-- ── AUTO-APPLY RECOMMENDATIONS ──────────────────────────────
-- These are safe, additive changes approved for automatic application.

{auto_apply_ddl_grouped_by_category}

-- ── MANUAL REVIEW RECOMMENDATIONS ───────────────────────────
-- These require architecture decisions or Enterprise Edition.
-- Uncomment and modify as needed after review.

-- [MANUAL REVIEW] {manual_review_ddl_commented_out}
```

Each DDL statement includes an inline comment with: rule ID, target object,
severity, and one-line rationale.

---

## Validation Checklist

Before finalizing any conversion run, verify:

- [ ] Every source block appears in `object_inventory.json`
- [ ] Every object has a corresponding entry in `conversion_log.json`
- [ ] Every generated SQL file has a meta provenance header
- [ ] Every non-trivial conversion has an inline annotation
- [ ] Every unsupported construct has a TODO block
- [ ] Every scored object has an entry in `fidelity_scores.json`
- [ ] Every quarantined object has an entry in `quarantine_inventory.json`
- [ ] `run_log.json` has entries for all 12 steps
- [ ] `optimization_report.json` has at least one recommendation per applicable category
- [ ] `optimization_ddl.sql` separates auto-apply from manual-review items
- [ ] No source construct is silently dropped (Guiding Principle #5)
