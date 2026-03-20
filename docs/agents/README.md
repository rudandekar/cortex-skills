# INFA2DBT Agent Documentation

This directory contains detailed documentation for each agent in the INFA2DBT pipeline.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 1: Conversion                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Agent 1          Agent 2          Agent 3          Agent 4        │
│  XML Parser  ───► Converter   ───► Fidelity    ───► Validation     │
│                       │            Scorer            Critique       │
│                       │               │                  │          │
│                       ▼               ▼                  ▼          │
│                   Cortex         Quarantine         Agent 5        │
│                   Search          Path            Phase Handoff    │
│                                                         │          │
│                                                    Gate 1          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 2: Stabilization                           │
│                    (Lift-and-shift in production)                   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 3-5: Optimization                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Agent 6                Agent 7A              Agent 7B              │
│  ROI Scorer   ────────► Config Optimizer ───► SQL Optimizer         │
│  (Tier assignment)      (Phase 4)             (Phase 5)             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Agent Documentation

| Agent | Name | Phase | Documentation |
|-------|------|-------|---------------|
| 1 | XML Parser | Conversion | [agent1-xml-parser.md](agent1-xml-parser.md) |
| 2 | Converter | Conversion | [agent2-converter.md](agent2-converter.md) |
| 3 | Fidelity Scorer | Conversion | [agent3-fidelity-scorer.md](agent3-fidelity-scorer.md) |
| 4 | Validation Critique | Conversion | [agent4-validation-critique.md](agent4-validation-critique.md) |
| 5 | Phase Handoff | Conversion | [agent5-phase-handoff.md](agent5-phase-handoff.md) |
| 6 | ROI Scorer | Optimization | [agent6-roi-scorer.md](agent6-roi-scorer.md) |
| 7 | SQL Optimizer | Optimization | [agent7-sql-optimizer.md](agent7-sql-optimizer.md) |

## Agent Responsibilities Summary

| Agent | Primary Question | Key Output |
|-------|------------------|------------|
| 1 | How do I decompose this XML? | Handoff JSONs per target |
| 2 | How do I convert to dbt SQL? | dbt models, schema, tests |
| 3 | Is output semantically correct? | Pass/Retry/Quarantine decision |
| 4 | Is code built correctly? | Compliance validation |
| 5 | Is the batch ready? | Gate 1 sign-off package |
| 6 | Which models to optimize first? | ROI tier assignments |
| 7 | How do I optimize this model? | Config + SQL optimizations |

## HITL Checkpoints

| Agent | Risk Level | Checkpoint |
|-------|-----------|------------|
| 1 | MEDIUM | Decomposition summary review |
| 2 | — | Autonomous |
| 3 | — | Autonomous (quarantine is automatic) |
| 4 | — | Autonomous (escalations flagged) |
| 5 | HIGH | Gate 1 sign-off required |
| 6 | MEDIUM | Tier assignment review |
| 7A | MEDIUM | Config changes validation |
| 7B | HIGH | Pattern C semantic rewrites |

## Routing Paths

### Standard Path
```
Agent 1 → Agent 2 → Agent 3 (pass) → Agent 4 (pass) → Agent 5 → Gate 1
```

### Retry Path
```
Agent 3 (fail) → Agent 2 (retry) → Agent 3 ...
Agent 4 (fail) → Agent 2 (retry) → Agent 3 → Agent 4 ...
```

### Quarantine Path
```
Agent 2 (missing coverage) → Quarantine
Agent 3 (max retries) → Quarantine → Human Review → Agent 4
```

### Escalation Path
```
Agent 4 (max retries) → Escalation → Gate 1 Review
```
