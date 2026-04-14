---
name: phase1-handoff
description: >
  Executes compilation checkpoints, collects workflow batches, generates Gate 1
  sign-off packages, and tracks completion state across the conversion pipeline.
  Use this skill when building or running checkpoints, generating handoff packages,
  collecting workflow batches, or preparing sign-off documents. Trigger on keywords:
  checkpoint, compile checkpoint, batch checkpoint, Gate 1, sign-off package,
  handoff report, workflow completion, forward reference resolution.
  This skill never touches SQL — it manages pipeline state only.
  Do NOT use for conversion — use infa-to-dbt-converter instead.
---

# Phase 1 Handoff (Agent 5)

Agent 5 manages pipeline state across workflows. It runs three types of checkpoints
and produces the Gate 1 sign-off package.

**Key responsibilities:**
- Checkpoint A: Per-workflow compile gate
- Checkpoint B: Every N workflows batch gate
- Checkpoint C: Full Phase 1 completion → Gate 1 sign-off package

Agent 5 has no HITL checkpoints — but it PRODUCES the artifacts that Gate 1
reviewers sign off on.

## Inputs

### Checkpoint A (per-workflow)
- **Required:** `workflow_name`, `model_paths`, `agent4_pass_signals`

### Checkpoint B (batch)
- **Required:** `workflow_batch`, `batch_number`, `total_batches`

### Checkpoint C (Gate 1)
- **Required:** `all_workflows`, `pipeline_config`

## Checkpoint A: Per-Workflow Compile Gate

Triggered immediately after ALL models for a single workflow pass Agent 4.

**Step A1:** Verify all models for this workflow have Agent 4 pass signals.

**Step A2:** Execute full workflow compile:
```bash
dbt compile --select tag:wf_{workflow_name}+
```
On success: log compile success. On forward reference error: log, add to forward_reference_queue, mark as `pending_forward_resolution`, continue. On other failure: log, mark as `checkpoint_a_failed`, escalate, continue.

**Step A3:** Run workflow-scoped tests:
```bash
dbt test --select tag:wf_{workflow_name} --store-failures
```

**Step A4:** Update workflow state with checkpoint timestamp, status, model counts, test summary, and forward references.

## Checkpoint B: Batch Gate (Every N Workflows)

Triggered after every N workflows complete Checkpoint A (default N=10).

**Step B1:** Resolve forward references — check if referenced models now exist, re-compile if so.

**Step B2:** Full batch compile:
```bash
dbt compile --select tag:batch_{batch_number}
```

**Step B3:** Batch-scoped tests:
```bash
dbt test --select tag:batch_{batch_number} --store-failures
```

**Step B4:** Generate batch report with: batch_number, batch_size, workflows list, models count, compile_status, test_summary (total/passed/failed), forward_references resolved/pending, escalation count.

**Step B5:** Pipeline can be configured to pause at Checkpoint B for human review via `checkpoint_b.auto_continue` in pipeline_config.yml.

## Checkpoint C: Gate 1 Sign-Off Package

Triggered after ALL workflows complete Checkpoint A.

**Step C1:** Final full compile: `dbt compile` — any failure is a hard stop.

**Step C2:** Full project test: `dbt test --store-failures`

**Step C3:** Generate sign-off manifest JSON with: gate metadata, summary (workflows_processed, models_converted, models_quarantined, conversion_rate, avg_fidelity), compile_status, test_status (total/passed/failed/rate), quarantine_summary, escalation_summary, unit_test_waivers, scheduling_stub_status.

**Step C4:** Generate supporting artifacts:

| Artifact | Path | Description |
|----------|------|-------------|
| Sign-off manifest | `docs/gate1/sign_off_manifest.json` | Full JSON |
| Human-readable summary | `docs/gate1/sign_off_summary.md` | Markdown |
| Failed tests report | `docs/gate1/failed_tests.csv` | All failures |
| Quarantine inventory | `docs/gate1/quarantine_inventory.csv` | Quarantined models |
| Escalation inventory | `docs/gate1/escalation_inventory.csv` | Agent 4 escalations |
| Scheduling stub | `docs/gate1/scheduling_stub.csv` | For human completion |

**Step C5:** Generate sign-off checklist:

```markdown
# Gate 1 Sign-Off Checklist
## Pre-requisites
- [ ] G1-1: All workflows processed
- [ ] G1-2: Full project compile passed
- [ ] G1-3: No pending forward references
## Required Reviews
- [ ] G1-4: Review quarantine inventory
- [ ] G1-5: Review escalation inventory
- [ ] G1-6: Review test failure report
- [ ] G1-7: Confirm unit test waivers
## Unit Test Coverage
- [ ] G1-11: Unit test pass rate ≥ 95%
- [ ] G1-12: All unit_test_waived models documented
## Scheduling Decisions (Human Required)
- [ ] Complete scheduling_stub.csv
- [ ] Assign domain tags
- [ ] Define inter-workflow dependencies
## Sign-Off
- [ ] Engineering Lead approval
- [ ] QA Lead approval (if test failures > 5%)
```

**Step C6: ⚠️ HITL CHECKPOINT — 🔴 HIGH — Gate 1 sign-off required.** Pipeline cannot proceed to Phase 2 until Engineering Lead signs checklist and quarantine models are reviewed.

## Error Handling

- **Forward reference resolution fails:** Keep in queue, add to C escalation, don't block.
- **Batch compile fails:** Log, identify failing models, continue collecting for C.
- **Full compile at C1 fails:** Hard stop, generate partial manifest with failure details.
- **Artifact generation fails:** Retry, flag for manual creation if persistent.

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
