# Agent 4: dbt Validation & Critique

**Skill Name:** `dbt-validation-critique`  
**Version:** 1.0.0  
**Phase:** 1 (Conversion)

## Purpose

Quality and compliance gate. Where Agent 3 asks "does it produce the right output?", Agent 4 asks **"is it built the right way?"**

A model can be semantically correct but still violate naming conventions, omit required tests, use wrong materialization, or have unresolved `ref()` calls.

**Additional Role:** Entry point for human-corrected quarantined models (bypasses Agents 1-3).

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_sql_path` | Yes | Path to dbt model file |
| `schema_yml_path` | Yes | Path to schema.yml entry |
| `handoff_json_path` | Yes | Original Agent 1 handoff |
| `agent3_pass_signal` | Yes | Agent 3's output with fidelity score |
| `coding_guidelines_path` | Yes | Version-locked coding guidelines |
| `reprocess_flag` | No | True if human-corrected resubmission |

## Outputs

| File | Location | Condition |
|------|----------|-----------|
| Pass signal + path | Checkpoint A | All checks pass |
| Structured critique | Agent 2 | Check fails, retries remain |
| HITL escalation log | `logs/agent4/escalations/` | Max retries exhausted |
| Validation log | `logs/agent4/` | Always |

## Workflow Steps

### Step 1: dbt Compile
```bash
dbt compile --select {model_name}+
```
- Catches SQL syntax errors, unresolved `ref()`, undeclared `source()`
- Forward references logged as warnings, not failures

### Step 2: Config Block Compliance

| Check | Expected |
|-------|----------|
| `materialized` present | Yes |
| `materialized` correct for prefix | `stg_`→view, `int_`→ephemeral, `mart_`→table |
| `schema` present | Yes |
| `tags` present | Yes |
| `tag:wf_{workflow}` present | Yes |
| `meta.source_workflow` present | Yes |
| `meta.generated_by` present | Yes |

### Step 3: SQL Quality Check

| Check | Rule |
|-------|------|
| CTE structure | All logic in CTEs |
| CTE naming | `source_*`, `{type}_*`, `final` pattern |
| `final` CTE present | Last CTE |
| No hardcoded schemas | Use `{{ source() }}` or `{{ ref() }}` |
| No `SELECT *` in intermediate | Only in source CTEs |
| Column aliases | snake_case, no reserved words |

### Step 4: Schema.yml Check

| Check | Rule |
|-------|------|
| Model name matches SQL | Exact match |
| Description present | Non-empty |
| All columns documented | Every `final` CTE column |
| `not_null` on PK columns | Required |
| `unique` on PK columns | Required |

### Step 5: Unit Test Validation
```bash
dbt test --select {model_name} type:unit
```
- Unit test failure is a **hard fail**
- Exception: `unit_test_waived` flag allows bypass

### Step 6: Routing Decision

**PASS:** All Steps 1-5 pass → Checkpoint A

**RETRY:** Any check fails AND `retry_count < 3` → Return to Agent 2

**ESCALATE:** Any check fails AND `retry_count = 3` → Flag for Gate 1 review

## Structured Critique Format

```json
{
  "model_name": "mart_dim_vendor_full",
  "retry_count": 2,
  "failing_checks": [
    {
      "check_id": "config.materialization_value",
      "severity": "error",
      "description": "Materialization 'view' incorrect for 'mart_' prefix",
      "location": "config() block",
      "guideline_reference": "§4.2",
      "suggested_fix": "Change to materialized='table'"
    }
  ],
  "passing_checks": ["dbt_compile", "cte_structure"],
  "instruction_to_agent2": "Correct only the failing checks. Do not regenerate from scratch."
}
```

## Reprocess Path (Human-Corrected Quarantine)

1. Agent 4 invoked directly — Agents 1-3 bypassed
2. Steps 1-5 run identically
3. TODO comment blocks must be removed
4. Pass: move to `models/converted/`
5. Fail: return critique to human (not Agent 2)
6. Max retry depth does NOT apply — unlimited human corrections

## Error Handling

| Condition | Action |
|-----------|--------|
| dbt compile timeout | Retry once, then hold pending |
| Forward reference | Log warning, do NOT fail |
| Schema.yml parse error | Hard fail, return critique |
| Guidelines version mismatch | Hard stop, escalate |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| None | — | Autonomous; escalations flagged for Gate 1 |
