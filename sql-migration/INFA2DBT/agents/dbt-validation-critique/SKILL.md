---
name: dbt-validation-critique
description: >
  Validates DBT models for compile success, config compliance, SQL quality, schema
  documentation, and unit test coverage. Use this skill when validating converted
  models against coding guidelines, checking DBT compile, reviewing config blocks,
  or generating structured critiques for retry cycles. Trigger on keywords: validate
  dbt model, compile check, config compliance, SQL quality, schema.yml validation,
  unit test coverage, coding guidelines.
  This is the quality and compliance gate — "is it built the right way?"
  Do NOT use for fidelity scoring — use conversion-fidelity-scorer instead.
---

# DBT Validation & Critique (Agent 4)

This skill is the quality and compliance gate. Where Agent 3 asks "does it produce
the right output?", Agent 4 asks "is it built the right way?" A model can be
semantically correct but still violate naming conventions, omit required tests,
use wrong materialization, or have unresolved `ref()` calls.

Agent 4 also produces structured critiques for Agent 2's retry cycle. Critiques
must be specific, actionable, and reference the relevant coding guideline.

**Agent 4 is also the entry point for human-corrected quarantined models** —
reprocessing bypasses Agents 1-3 and goes directly to Agent 4.

## Inputs

- **Required:** `model_sql_path` — Path to DBT model file
- **Required:** `schema_yml_path` — Path to schema.yml entry
- **Required:** `handoff_json_path` — Original Agent 1 handoff
- **Required:** `agent3_pass_signal` — Agent 3's output with fidelity score
- **Required:** `coding_guidelines_path` — Version-locked coding guidelines
- **Optional:** `reprocess_flag` — True if human-corrected quarantine resubmission

## Pre-conditions

- Agent 3 fidelity score >= threshold (or reprocess flag set)
- DBT project state includes all previously passed models
- Coding guidelines version matches pipeline config

## Workflow

### Step 1: DBT compile (per-workflow scope)

```bash
dbt compile --select {model_name}+
```

The `+` suffix includes all upstream models via `ref()`.

**What this catches:**
- SQL syntax errors
- Unresolved `ref()` calls
- Undeclared `source()` references
- Jinja template errors

**Pass:** Exit code 0, compiled SQL in `target/compiled/`

**Fail:** Capture full error, map to specific line, add to critique. STOP — do not
proceed to Step 2.

**Forward reference handling:** If `ref()` points to a not-yet-converted model,
log as `forward_reference_warning` (not failure). Resolution expected at
Checkpoint B or C.

### Step 2: Config block compliance check

Validate config block against coding guidelines:

| Check | Expected | Fail Condition |
|-------|----------|----------------|
| `materialized` present | Yes | Missing or misspelled |
| `materialized` correct for prefix | `stg_`→view, `int_`→ephemeral, `mart_`→table | Mismatch |
| `schema` present | Yes | Missing |
| `tags` present | Yes | Missing |
| `tag:wf_{workflow}` present | Yes | Missing workflow tag |
| `tag:TODO_domain` or resolved | Yes | Neither present |
| `tag:TODO_freq` or resolved | Yes | Neither present |
| `tag:review_pending` absent | Yes (pass path) | Present on passed model |
| `meta.source_workflow` present | Yes | Missing |
| `meta.generated_by` present | Yes | Missing |
| `meta.generation_timestamp` present | Yes | Missing |
| `unique_key` (if incremental) | Yes | Missing or TODO placeholder |

### Step 3: SQL quality and naming check

Validate SQL body structure:

| Check | Rule | Fail Condition |
|-------|------|----------------|
| CTE structure | All logic in CTEs | Inline subquery in final SELECT |
| CTE naming | `source_*`, `{type}_*`, `final` pattern | Non-conformant name |
| `final` CTE present | Last CTE, `SELECT * FROM final` terminal | Absent |
| No hardcoded schemas | Use `{{ source() }}` or `{{ ref() }}` | Hardcoded schema.table |
| No `SELECT *` in intermediate | Only in source CTEs and terminal | `SELECT *` in transform CTE |
| Column aliases | snake_case, no spaces, no reserved words | Violation |
| No deprecated syntax | Correct Snowflake syntax | QUALIFY misuse, wrong FLATTEN |
| Cluster_by stub | Present for table/incremental | Missing for applicable types |

### Step 4: Schema.yml and test coverage check

| Check | Rule | Fail Condition |
|-------|------|----------------|
| Model name matches SQL | Exact match | Mismatch |
| `description` present | Non-empty | Missing or empty |
| `meta.source_workflow` matches | Same as config block | Mismatch |
| All columns documented | Every `final` CTE column in schema | Missing columns |
| `not_null` on PK columns | Required | Missing |
| `unique` on PK columns | Required | Missing |
| Column descriptions | "Source: table.column via type" | Non-conformant |

### Step 5: Unit test validation

```bash
dbt test --select {model_name} type:unit
```

| Check | Rule | Fail Condition |
|-------|------|----------------|
| Unit test file exists | `tests/unit/{model}_unit.yml` | Absent without `unit_test_missing` flag |
| Adequate test count | ≥1 test per transformation type | Type has zero tests |
| No unacknowledged gaps | `# COVERAGE GAP` reviewed | Unacknowledged gap |
| Tests execute cleanly | Exit code 0 | Any test fails — HARD FAIL |
| Fixtures use valid columns | Only columns in `final` CTE | Column mismatch |

**Unit test failure is a hard fail** — model cannot proceed if any test fails.

**Exception:** If `unit_test_missing = True` from Agent 3, log `unit_test_waived`,
add `tag:unit_test_waived` to model, include in Gate 1 sign-off report.

### Step 6: Routing decision

**PASS (to Compile Checkpoint A):**
- All Steps 1-5 pass
- Log pass with check results
- Confirm model in `models/converted/`
- For reprocess: move from `models/review_pending/` to `models/converted/`

**RETRY (return to Agent 2):**
- Any check fails AND `retry_count < 3`
- Increment retry_count (shared pool with Agent 3)
- Produce structured critique
- Return to Agent 2 for targeted correction

**ESCALATE to HITL:**
- Any check fails AND `retry_count = 3`
- Do NOT quarantine (that's Agent 3's role)
- Log as `agent4_escalation`
- Flag in Gate 1 sign-off report
- Add `tag:agent4_escalated` to model
- Leave in `models/converted/`

### Step 7: Generate structured critique (for retry)

```json
{
  "model_name": "mart_dim_vendor_full",
  "retry_count": 2,
  "failing_checks": [
    {
      "check_id": "unit_test.execution",
      "severity": "error",
      "description": "Unit test 'test_filter_excludes_inactive' failed",
      "location": "tests/unit/mart_dim_vendor_full_unit.yml",
      "guideline_reference": "INFA2DBT_coding_guidelines.md §7.1",
      "suggested_fix": "Check WHERE clause in filtered_active_vendors CTE"
    },
    {
      "check_id": "config.materialization_value",
      "severity": "error",
      "description": "Materialization 'view' incorrect for 'mart_' prefix",
      "location": "config() block",
      "guideline_reference": "INFA2DBT_coding_guidelines.md §4.2",
      "suggested_fix": "Change to materialized='table'"
    }
  ],
  "passing_checks": ["dbt_compile", "cte_structure", "naming_conventions"],
  "instruction_to_agent2": "Correct only the failing checks. Do not regenerate from scratch."
}
```

The `instruction_to_agent2` field prevents full regeneration (wastes retries).

## Output Specification

| File | Location | Format | Condition |
|------|----------|--------|-----------|
| Pass signal + model path | Checkpoint A | JSON | All checks pass |
| Structured critique | Agent 2 | JSON | Any check fails, retries remain |
| HITL escalation log | `logs/agent4/escalations/` | JSON | Max retries exhausted |
| Validation log | `logs/agent4/` | JSON | Always |

Inline summary must include:
- Model validated: {name}
- Compile: PASS/FAIL
- Config checks: {passed}/{total}
- SQL quality checks: {passed}/{total}
- Schema.yml checks: {passed}/{total}
- Unit tests: {passed}/{total}
- Decision: PASS / RETRY / ESCALATE

## Error Handling

### `dbt compile` timeout
Retry compile once. If second attempt times out, log as infrastructure issue.
Hold model pending. Do NOT count against retry_count.

### `dbt compile` forward reference
Log as `forward_reference_warning`. Do NOT fail. Resolve at Checkpoint B/C.

### Schema.yml parse error
Agent 2 output error. Hard fail Step 4. Return with critique specifying YAML
parse error location.

### Guidelines version mismatch
Hard stop. Model's `meta.generated_by` version doesn't match current guidelines.
Log and escalate — guidelines updated mid-run requires project-level decision.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| None | — | Agent 4 operates autonomously; escalation flags for Gate 1 review |

## Reference Tables

### Retry Count Rules

- Shared pool: Agent 3 + Agent 4 combined ≤ 3 retries
- Example: If Agent 3 uses 2 retries, Agent 4 has 1 remaining
- On retry: targeted correction only, not full regeneration

### Reprocess Path (Human-Corrected Quarantine)

1. Agent 4 invoked directly — Agents 1-3 bypassed
2. Steps 1-5 run identically
3. TODO comment blocks must be removed before resubmission
4. Pass: move to `models/converted/`, update mapping table
5. Fail: return critique to human (not Agent 2)
6. Max retry depth does NOT apply — human correction unlimited
