---
name: phase1-handoff
version: 1.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

description: >
  Executes compilation checkpoints, collects workflow batches, generates Gate 1
  sign-off packages, and tracks completion state across the conversion pipeline.
  Use this skill when building or running checkpoints, generating handoff packages,
  collecting workflow batches, or preparing sign-off documents. Trigger on keywords:
  checkpoint, compile checkpoint, batch checkpoint, Gate 1, sign-off package,
  handoff report, workflow completion, forward reference resolution.
  This skill never touches SQL — it manages pipeline state only.
  Do NOT use for conversion — use infa-to-dbt-converter instead.

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md]
---

# Phase 1 Handoff (Agent 5)

Agent 5 operates at a higher level than Agents 1-4. Where those agents process
individual models, Agent 5 manages pipeline state across workflows. It runs three
types of checkpoints and produces the Gate 1 sign-off package.

**Key responsibilities:**
- Checkpoint A: Per-workflow compile gate
- Checkpoint B: Every N workflows batch gate
- Checkpoint C: Full Phase 1 completion → Gate 1 sign-off package

Agent 5 has no HITL checkpoints — but it PRODUCES the artifacts that Gate 1
reviewers sign off on.

## Inputs

### Checkpoint A (per-workflow)
- **Required:** `workflow_name` — Workflow that just completed Agent 4
- **Required:** `model_paths` — List of all model paths for this workflow
- **Required:** `agent4_pass_signals` — Pass signals from Agent 4 for each model

### Checkpoint B (batch)
- **Required:** `workflow_batch` — List of workflow names in this batch
- **Required:** `batch_number` — Current batch index (1-indexed)
- **Required:** `total_batches` — Expected total batches

### Checkpoint C (Gate 1)
- **Required:** `all_workflows` — Complete list of processed workflows
- **Required:** `pipeline_config` — Phase 1 configuration

## Checkpoint A: Per-Workflow Compile Gate

Triggered: Immediately after ALL models for a single workflow pass Agent 4.

### Step A1: Verify workflow-level completion

```python
workflow_models = [m for m in all_models if m['workflow_name'] == workflow_name]
passed_models = [m for m in agent4_pass_signals if m['model_name'] in workflow_models]

assert len(passed_models) == len(workflow_models), \
    f"Workflow incomplete: {len(passed_models)}/{len(workflow_models)} models passed"
```

### Step A2: Execute full workflow compile

```bash
dbt compile --select tag:wf_{workflow_name}+
```

This compiles all models tagged with the workflow, plus upstream dependencies.

**Pass:** Exit code 0. Log compile success.

**Fail (forward reference):** If error is unresolved `ref()`:
1. Log `forward_reference_{model_name}` 
2. Add to `forward_reference_queue`
3. Mark workflow as `pending_forward_resolution`
4. Continue to next workflow — do NOT block

**Fail (other):** Log error. Mark workflow as `checkpoint_a_failed`. Add to
escalation queue. Continue to next workflow.

### Step A3: Run workflow-scoped tests

```bash
dbt test --select tag:wf_{workflow_name} --store-failures
```

This runs all tests (unit + schema) for workflow models.

**Pass:** Log test summary.

**Fail:** Log failed tests. If critical test fails, mark workflow for escalation.
Do NOT block pipeline.

### Step A4: Update workflow state

```python
workflow_state = {
    'workflow_name': workflow_name,
    'checkpoint_a_timestamp': datetime.utcnow().isoformat(),
    'checkpoint_a_status': 'passed',  # or 'pending_forward', 'failed'
    'models_count': len(workflow_models),
    'models_passed': len(passed_models),
    'test_summary': test_results,
    'forward_references': forward_refs_if_any
}

update_pipeline_state(workflow_state)
```

## Checkpoint B: Batch Gate (Every N Workflows)

Triggered: After every N workflows complete Checkpoint A (default N=10).

### Step B1: Resolve forward references

```python
forward_ref_queue = get_forward_reference_queue()

for forward_ref in forward_ref_queue:
    # Check if referenced model now exists
    referenced_model = forward_ref['referenced_model']
    if model_exists_and_passed(referenced_model):
        # Re-run compile for the model with forward ref
        dbt_compile(forward_ref['model_name'])
        mark_forward_ref_resolved(forward_ref)
    else:
        # Still pending — leave in queue
        pass
```

### Step B2: Full batch compile

```bash
dbt compile --select tag:batch_{batch_number}
```

This compiles all models in the current batch to catch any cross-workflow
dependency issues.

### Step B3: Run batch-scoped tests

```bash
dbt test --select tag:batch_{batch_number} --store-failures
```

### Step B4: Generate batch report

```json
{
  "batch_number": 3,
  "batch_size": 10,
  "workflows_in_batch": ["WF_VENDOR_FULL", "WF_CUSTOMER_INCR", ...],
  "models_in_batch": 47,
  "compile_status": "passed",
  "test_summary": {
    "total": 312,
    "passed": 308,
    "failed": 4,
    "skipped": 0
  },
  "forward_references_resolved": 12,
  "forward_references_pending": 3,
  "escalations": 2,
  "timestamp": "2026-03-15T16:45:00Z"
}
```

### Step B5: ⚠️ Batch pause point

At Checkpoint B, the pipeline can be configured to pause for human review:

```yaml
# pipeline_config.yml
checkpoint_b:
  auto_continue: true  # or false for manual gate
  notify_on_batch_complete: true
  notify_channel: "slack://infa2dbt-alerts"
```

If `auto_continue: false`, Agent 5 pauses and waits for explicit continue signal.

## Checkpoint C: Gate 1 Sign-Off Package

Triggered: After ALL workflows complete Checkpoint A (or all batches complete B).

### Step C1: Final full compile

```bash
dbt compile
```

This compiles the entire project to ensure no remaining forward references.

**Any failure here is a hard stop.** All forward references must be resolved
before Gate 1.

### Step C2: Full project test run

```bash
dbt test --store-failures
```

### Step C3: Generate sign-off manifest

```json
{
  "gate_id": "GATE_1",
  "gate_name": "Phase 1 Completion — Engineering Lead Sign-Off",
  "generated_timestamp": "2026-03-15T18:00:00Z",
  "pipeline_version": "1.1.0",
  "coding_guidelines_version": "v2.3",
  
  "summary": {
    "workflows_processed": 47,
    "models_converted": 142,
    "models_quarantined": 8,
    "models_escalated": 3,
    "total_conversion_rate": "91.5%",
    "avg_fidelity_score": 0.92
  },
  
  "compile_status": {
    "full_project_compile": "passed",
    "forward_references_resolved": 15,
    "forward_references_pending": 0
  },
  
  "test_status": {
    "total_tests": 1847,
    "passed": 1802,
    "failed": 45,
    "skipped": 0,
    "failure_rate": "2.4%"
  },
  
  "quarantine_summary": {
    "count": 8,
    "reasons": {
      "fidelity_retries_exhausted": 5,
      "missing_corpus_coverage": 2,
      "reference_sql_failure": 1
    },
    "models": [
      {"name": "mart_legacy_pricing", "reason": "..."},
      ...
    ]
  },
  
  "escalation_summary": {
    "count": 3,
    "models": [
      {"name": "int_complex_router", "reason": "..."},
      ...
    ]
  },
  
  "unit_test_waivers": {
    "count": 12,
    "models": ["stg_vendor_raw", ...]
  },
  
  "scheduling_stub_status": {
    "total_workflows": 47,
    "human_completed": 0,
    "pending_completion": 47
  }
}
```

### Step C4: Generate supporting artifacts

| Artifact | Path | Description |
|----------|------|-------------|
| Sign-off manifest | `docs/gate1/sign_off_manifest.json` | JSON above |
| Human-readable summary | `docs/gate1/sign_off_summary.md` | Markdown for review |
| Failed tests report | `docs/gate1/failed_tests.csv` | All test failures |
| Quarantine inventory | `docs/gate1/quarantine_inventory.csv` | All quarantined models |
| Escalation inventory | `docs/gate1/escalation_inventory.csv` | All Agent 4 escalations |
| Coverage report | `docs/gate1/coverage_report.html` | DBT docs coverage |
| Scheduling stub | `docs/gate1/scheduling_stub.csv` | For human completion |

### Step C5: Generate sign-off checklist

```markdown
# Gate 1 Sign-Off Checklist

## Pre-requisites
- [ ] G1-1: All workflows processed (47/47)
- [ ] G1-2: Full project compile passed
- [ ] G1-3: No pending forward references

## Required Reviews
- [ ] G1-4: Review quarantine inventory (8 models)
- [ ] G1-5: Review escalation inventory (3 models)
- [ ] G1-6: Review test failure report (45 failures)
- [ ] G1-7: Confirm unit test waivers (12 models)

## Unit Test Coverage (NEW)
- [ ] G1-11: Unit test pass rate ≥ 95%, zero models with failures
- [ ] G1-12: All unit_test_waived models documented with reason

## Scheduling Decisions (Human Required)
- [ ] Complete scheduling_stub.csv for all 47 workflows
- [ ] Assign domain tags
- [ ] Define inter-workflow dependencies

## Sign-Off
- [ ] Engineering Lead approval
- [ ] QA Lead approval (if test failures > 5%)
- [ ] Date: _______________
```

### Step C6: ⚠️ HITL CHECKPOINT — 🔴 HIGH — Gate 1 sign-off required

**This is a HARD GATE.** Pipeline cannot proceed to Phase 2 until:
1. Engineering Lead signs checklist
2. Quarantine models are reviewed (proceed, fix, or defer)
3. Scheduling stub is completed

## Output Specification

### Checkpoint A Output
| File | Location | Description |
|------|----------|-------------|
| Workflow state | `logs/checkpoint_a/` | Per-workflow state JSON |
| Forward ref queue | `state/forward_refs.json` | Pending resolutions |

### Checkpoint B Output
| File | Location | Description |
|------|----------|-------------|
| Batch report | `logs/checkpoint_b/` | Per-batch summary JSON |
| Cumulative state | `state/pipeline_state.json` | Running totals |

### Checkpoint C Output
| File | Location | Description |
|------|----------|-------------|
| Sign-off manifest | `docs/gate1/sign_off_manifest.json` | Full JSON manifest |
| Sign-off summary | `docs/gate1/sign_off_summary.md` | Human-readable |
| All supporting CSVs | `docs/gate1/` | Inventories and stubs |
| Coverage report | `docs/gate1/coverage_report.html` | DBT docs |

## Error Handling

### Forward reference resolution fails
Keep in queue. Add to Checkpoint C escalation. Do not block other workflows.

### Batch compile fails
Log batch as failed. Identify specific failing models. Continue collecting for
Checkpoint C report.

### Full project compile at C1 fails
Hard stop. This means unresolved dependencies. Generate partial sign-off manifest
with failure details. Escalate to human.

### Sign-off artifact generation fails
Retry generation. If persistent failure, generate minimal manifest with error
details and flag for manual sign-off creation.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Step C6 | 🔴 HIGH | Gate 1 sign-off required before Phase 2 |

## Reference Tables

### Checkpoint Triggers

| Checkpoint | Trigger Condition |
|------------|-------------------|
| A | All models for one workflow pass Agent 4 |
| B | Every N workflows complete A (default N=10) |
| C | All workflows complete A |

### Gate 1 Sign-Off Authorities

| Role | Approval Scope |
|------|----------------|
| Engineering Lead | Full sign-off |
| QA Lead | Required if test failure rate > 5% |
| Data Architect | Optional review of quarantine |
