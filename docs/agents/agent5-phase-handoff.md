# Agent 5: Phase 1 Handoff

**Skill Name:** `phase1-handoff`  
**Version:** 1.0.0  
**Phase:** 1 (Conversion)

## Purpose

Operates at a higher level than Agents 1-4. Where those agents process individual models, Agent 5 **manages pipeline state across workflows**. It runs three types of checkpoints and produces the Gate 1 sign-off package.

**Key Responsibilities:**
- Checkpoint A: Per-workflow compile gate
- Checkpoint B: Every N workflows batch gate
- Checkpoint C: Full Phase 1 completion → Gate 1 sign-off

## Checkpoint Types

### Checkpoint A: Per-Workflow Compile Gate

**Trigger:** All models for a single workflow pass Agent 4

**Steps:**
1. Verify workflow-level completion
2. Execute full workflow compile: `dbt compile --select tag:wf_{workflow}+`
3. Run workflow-scoped tests: `dbt test --select tag:wf_{workflow}`
4. Update workflow state

**Forward Reference Handling:**
- Log `forward_reference_{model}`, add to queue
- Mark workflow as `pending_forward_resolution`
- Continue to next workflow — do NOT block

### Checkpoint B: Batch Gate (Every N Workflows)

**Trigger:** Every N workflows complete Checkpoint A (default N=10)

**Steps:**
1. Resolve forward references
2. Full batch compile: `dbt compile --select tag:batch_{N}`
3. Run batch-scoped tests
4. Generate batch report
5. Optional pause point (configurable)

**Batch Report:**
```json
{
  "batch_number": 3,
  "batch_size": 10,
  "models_in_batch": 47,
  "compile_status": "passed",
  "forward_references_resolved": 12,
  "forward_references_pending": 3
}
```

### Checkpoint C: Gate 1 Sign-Off Package

**Trigger:** All workflows complete Checkpoint A

**Steps:**
1. Final full compile: `dbt compile`
2. Full project test run: `dbt test --store-failures`
3. Generate sign-off manifest
4. Generate supporting artifacts
5. Generate sign-off checklist
6. **HITL CHECKPOINT** — Gate 1 sign-off required

## Gate 1 Sign-Off Manifest

```json
{
  "gate_id": "GATE_1",
  "summary": {
    "workflows_processed": 47,
    "models_converted": 142,
    "models_quarantined": 8,
    "total_conversion_rate": "91.5%",
    "avg_fidelity_score": 0.92
  },
  "compile_status": {
    "full_project_compile": "passed",
    "forward_references_pending": 0
  },
  "test_status": {
    "total_tests": 1847,
    "passed": 1802,
    "failed": 45,
    "failure_rate": "2.4%"
  },
  "quarantine_summary": {...},
  "escalation_summary": {...}
}
```

## Gate 1 Artifacts

| Artifact | Path | Description |
|----------|------|-------------|
| Sign-off manifest | `docs/gate1/sign_off_manifest.json` | Full JSON |
| Human-readable summary | `docs/gate1/sign_off_summary.md` | Markdown |
| Failed tests report | `docs/gate1/failed_tests.csv` | All failures |
| Quarantine inventory | `docs/gate1/quarantine_inventory.csv` | Quarantined models |
| Escalation inventory | `docs/gate1/escalation_inventory.csv` | Agent 4 escalations |
| Coverage report | `docs/gate1/coverage_report.html` | dbt docs |
| Scheduling stub | `docs/gate1/scheduling_stub.csv` | For human completion |

## Gate 1 Sign-Off Checklist

```markdown
## Pre-requisites
- [ ] G1-1: All workflows processed
- [ ] G1-2: Full project compile passed
- [ ] G1-3: No pending forward references

## Required Reviews
- [ ] G1-4: Review quarantine inventory
- [ ] G1-5: Review escalation inventory
- [ ] G1-6: Review test failure report
- [ ] G1-7: Confirm unit test waivers

## Scheduling Decisions (Human Required)
- [ ] Complete scheduling_stub.csv
- [ ] Assign domain tags
- [ ] Define inter-workflow dependencies

## Sign-Off
- [ ] Engineering Lead approval
- [ ] QA Lead approval (if test failures > 5%)
```

## Error Handling

| Condition | Action |
|-----------|--------|
| Forward reference unresolved | Keep in queue, add to C escalation |
| Batch compile fails | Log, continue collecting for C report |
| Full compile at C1 fails | Hard stop, generate partial manifest |
| Artifact generation fails | Retry, generate minimal with errors |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| Checkpoint C / Step C6 | HIGH | Gate 1 sign-off required before Phase 2 |

## Gate 1 Sign-Off Authorities

| Role | Approval Scope |
|------|----------------|
| Engineering Lead | Full sign-off |
| QA Lead | Required if test failure rate > 5% |
| Data Architect | Optional quarantine review |
