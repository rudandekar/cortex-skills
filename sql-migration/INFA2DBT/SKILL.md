---
name: infa2dbt-accelerator
description: >
  Orchestrates the full INFA2DBT pipeline: converting Informatica PowerCenter
  workflows to production-ready DBT models on Snowflake. Use this skill when
  converting Informatica to DBT, migrating PowerCenter workflows, or running
  the INFA2DBT accelerator. Trigger on keywords: informatica to dbt, powermart
  conversion, workflow migration, INFA2DBT, powercentre to snowflake,
  informatica migration, dbt conversion project.
---

# INFA2DBT Accelerator v2.0 — Main Orchestrator

This skill orchestrates the complete Informatica-to-DBT conversion pipeline. It
coordinates 7 specialized agents across 5 phases with 6 human checkpoints.

Features: RAG-enhanced conversion via Cortex Search corpus, persistent state in
Snowflake tables (PIPELINE_STATE, MODEL_REGISTRY, FIDELITY_SCORES), model
registry with fidelity tracking, and resume-from-failure with Snowflake-backed state.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    INFA2DBT ACCELERATOR PIPELINE v2.0                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PRE-FLIGHT                           ─────────────── Gate 0 (Human)        │
│                                                                             │
│  PHASE 1: CONVERSION (RAG-Enhanced)                                         │
│    Agent 1: infa-xml-parser           → Decompose XML to handoffs           │
│    Agent 2: infa-to-dbt-converter     → Generate DBT SQL (+ Corpus Search)  │
│    Agent 3: conversion-fidelity-scorer→ Score correctness → Snowflake       │
│    Agent 4: dbt-validation-critique   → Validate compliance                 │
│    Agent 5: phase1-handoff            → Checkpoint A/B/C ─── Gate 1 (Human) │
│                                                                             │
│  PHASE 2: STABILIZATION               ─────────────── Gate 2 (Human)        │
│    (Lift-and-shift to production, 2+ business cycles)                       │
│                                                                             │
│  PHASE 3: ROI ANALYSIS                                                      │
│    Agent 6: roi-subgraph-scorer       → Tier 1/2/3 ─────── Gate 3 (Human)   │
│                                                                             │
│  PHASE 4: CONFIG OPTIMIZATION                                               │
│    Agent 7A: dbt-sql-optimizer (config)→ Clustering ─── Gate 4 (Human)      │
│                                                                             │
│  PHASE 5: SQL OPTIMIZATION                                                  │
│    Agent 7B: dbt-sql-optimizer (SQL)  → Rewrites ─────── Gate 5 (Human)     │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════    │
│  PERSISTENT STATE: INFA2DBT_DB.PIPELINE.PIPELINE_STATE                      │
│  MODEL REGISTRY:   INFA2DBT_DB.PIPELINE.MODEL_REGISTRY                      │
│  FIDELITY SCORES:  INFA2DBT_DB.PIPELINE.FIDELITY_SCORES                     │
│  RAG CORPUS:       INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Snowflake Objects

**Load** references/snowflake-objects.md for table DDLs and Cortex Search service definition.

## RAG Corpus Search

**Load** references/rag-corpus.md for corpus search API and example insertion patterns.

## State Management

**Load** references/state-management.md for pipeline state CRUD operations.

## Invocation Modes

- **Full Pipeline:** `Run INFA2DBT accelerator on /path/to/workflows/`
- **Phase-Specific:** `Run INFA2DBT Phase 1 on /path/to/workflow.xml`
- **Single Agent:** `Run INFA2DBT Agent 1 on /path/to/workflow.xml`
- **Resume:** Query PIPELINE_STATE for failed runs, then `Resume INFA2DBT run {run_id}`

## Inputs

- **Required:** `workflow_xml_path` — Single XML or directory of XMLs
- **Required:** `dbt_project_path` — Target DBT project root
- **Required:** `coding_guidelines_path` — Version-locked coding guidelines
- **Optional:** `corpus_examples_dir` — Additional labelled examples (auto-ingested)
- **Optional:** `pipeline_config` — Custom thresholds and settings

## Pre-conditions

- Gate 0 (Pre-flight) complete: corpus_coverage.csv, scheduling_audit.xlsx reviewed, coding guidelines finalized
- DBT project initialized
- Snowflake connection active with sandbox access
- INFA2DBT_DB.PIPELINE schema accessible

## Phase 1 Workflow (v2.0)

### Step 1: Initialize pipeline state

Register a new run in PIPELINE_STATE with status IN_PROGRESS and phase CONVERSION.

### Step 2: Process workflows with RAG-enhanced conversion

For each workflow XML:
1. **Agent 1** parses and decomposes into per-target handoffs
2. Search corpus for relevant transformation patterns per type
3. **Agent 2** converts to DBT SQL with corpus examples as context
4. Register model in MODEL_REGISTRY
5. **Agent 3** scores fidelity, records to FIDELITY_SCORES
6. **Agent 4** validates compliance
7. Continue with Agent 5 checkpoints

### Step 3: Update pipeline status

On completion, update PIPELINE_STATE to COMPLETED. On failure, record ERROR_MSG and set status to FAILED.

## Error Recovery

Query PIPELINE_STATE for failed/in-progress runs. Find unprocessed models via MODEL_REGISTRY where FIDELITY_STATUS = 'PENDING'. On retry exhaustion (max_retries exceeded), set FIDELITY_STATUS to 'QUARANTINED'.

## Output Specification

| Location | Description |
|----------|-------------|
| `INFA2DBT_DB.PIPELINE.PIPELINE_STATE` | Pipeline run tracking |
| `INFA2DBT_DB.PIPELINE.MODEL_REGISTRY` | All converted models + SQL |
| `INFA2DBT_DB.PIPELINE.FIDELITY_SCORES` | Quality metrics |
| `models/converted/*.sql` | Local DBT model files |
| `docs/gate{N}/` | Per-gate sign-off artifacts |

## HITL Checkpoints Summary

| Gate | Phase | Risk | Approval Required |
|------|-------|------|-------------------|
| 0 | Pre-flight | 🟡 MEDIUM | Corpus + Scheduling audit |
| 1 | Phase 1 | 🔴 HIGH | Engineering Lead sign-off |
| 2 | Phase 2 | 🟡 MEDIUM | Stabilization completion |
| 3 | Phase 3 | 🟡 MEDIUM | ROI tier confirmation |
| 4 | Phase 4 | 🟡 MEDIUM | Config changes validation |
| 5 | Phase 5 | 🔴 HIGH | Each Pattern C rewrite |

## Sub-Skills Reference

| Skill | Agent | Purpose |
|-------|-------|---------|
| `infa-xml-parser` | 1 | Parse XML, decompose workflows |
| `infa-to-dbt-converter` | 2 | Generate DBT SQL (RAG-enhanced) |
| `conversion-fidelity-scorer` | 3 | Score correctness → Snowflake |
| `dbt-validation-critique` | 4 | Validate compliance |
| `phase1-handoff` | 5 | Checkpoints and gates |
| `roi-subgraph-scorer` | 6 | ROI tier analysis |
| `dbt-sql-optimizer` | 7 | Optimization (A+B) |

## Monitoring Queries

**Load** references/monitoring-queries.md for pipeline monitoring SQL.
