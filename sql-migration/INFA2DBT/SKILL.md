---
name: infa2dbt-accelerator
version: 2.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

description: >
  Orchestrates the full INFA2DBT pipeline: converting Informatica PowerCenter
  workflows to production-ready DBT models on Snowflake. Use this skill when
  converting Informatica to DBT, migrating PowerCenter workflows, or running
  the INFA2DBT accelerator. Trigger on keywords: informatica to dbt, powermart
  conversion, workflow migration, INFA2DBT, powercentre to snowflake,
  informatica migration, dbt conversion project.
  
  v2.0 Features:
  - RAG-enhanced conversion using Cortex Search corpus
  - Persistent state in Snowflake tables (PIPELINE_STATE, MODEL_REGISTRY, FIDELITY_SCORES)
  - Model registry with fidelity tracking
  - Resume-from-failure with Snowflake-backed state

compatibility:
  tools: [bash, read, write, snowflake_sql_execute, task]
  context: [CLAUDE.md, PROJECT.md, INFA2DBT_coding_guidelines.md]
  snowflake_objects:
    database: INFA2DBT_DB
    schema: PIPELINE
    tables: [PIPELINE_STATE, MODEL_REGISTRY, FIDELITY_SCORES, INFA2DBT_CORPUS]
    cortex_search: INFA2DBT_CORPUS_SEARCH
---

# INFA2DBT Accelerator v2.0 — Main Orchestrator

This skill orchestrates the complete Informatica-to-DBT conversion pipeline. It
coordinates 7 specialized agents across 5 phases with 6 human checkpoints.

## New in v2.0

### RAG-Enhanced Conversion
Agent 2 now uses Cortex Search to find relevant conversion examples from the
`INFA2DBT_CORPUS` before generating DBT code. This improves conversion quality
by leveraging labelled patterns.

### Persistent State Store
All pipeline state is now stored in Snowflake tables:
- `PIPELINE_STATE` — Track pipeline runs, status, phases
- `MODEL_REGISTRY` — Track all generated DBT models with SQL content
- `FIDELITY_SCORES` — Track conversion quality metrics per model

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

### Tables

```sql
-- Pipeline run tracking
INFA2DBT_DB.PIPELINE.PIPELINE_STATE (
    RUN_ID VARCHAR(64) PRIMARY KEY,
    WORKFLOW_NAME VARCHAR(256),
    XML_FILE_PATH VARCHAR(1024),
    STATUS VARCHAR(32),        -- PENDING, IN_PROGRESS, COMPLETED, FAILED
    CURRENT_PHASE VARCHAR(64), -- phase_1, phase_2, etc.
    CURRENT_GATE VARCHAR(32),
    STARTED_AT TIMESTAMP_NTZ,
    COMPLETED_AT TIMESTAMP_NTZ,
    ERROR_MSG TEXT,
    RETRY_COUNT INTEGER,
    METADATA VARIANT,
    CREATED_BY VARCHAR(128),
    UPDATED_AT TIMESTAMP_NTZ
)

-- Model registry
INFA2DBT_DB.PIPELINE.MODEL_REGISTRY (
    MODEL_ID VARCHAR(64) PRIMARY KEY,
    RUN_ID VARCHAR(64),
    MODEL_NAME VARCHAR(256),
    SOURCE_WORKFLOW VARCHAR(256),
    SOURCE_MAPPING VARCHAR(256),
    TARGET_TABLE VARCHAR(256),
    TARGET_SCHEMA VARCHAR(256),
    SQL_CONTENT TEXT,
    SCHEMA_YML TEXT,
    UNIT_TEST_YML TEXT,
    TRANSFORMATION_TYPES ARRAY,
    COLUMN_COUNT INTEGER,
    CTE_COUNT INTEGER,
    FIDELITY_STATUS VARCHAR(32),
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ
)

-- Fidelity scores
INFA2DBT_DB.PIPELINE.FIDELITY_SCORES (
    SCORE_ID VARCHAR(64) PRIMARY KEY,
    MODEL_ID VARCHAR(64),
    RUN_ID VARCHAR(64),
    OVERALL_SCORE FLOAT,
    STRUCTURE_SCORE FLOAT,
    SEMANTICS_SCORE FLOAT,
    TEST_COVERAGE FLOAT,
    TRANSFORMATION_COVERAGE FLOAT,
    COLUMN_MAPPING_ACCURACY FLOAT,
    LINEAGE_PRESERVED BOOLEAN,
    FLAGGED_ISSUES ARRAY,
    REVIEWER_NOTES TEXT,
    REVIEWED_BY VARCHAR(128),
    REVIEWED_AT TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ
)

-- RAG corpus for conversion examples
INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS (
    EXAMPLE_ID VARCHAR(64) PRIMARY KEY,
    TRANSFORMATION_TYPE VARCHAR(64),  -- Expression, Filter, Aggregator, etc.
    INFA_PATTERN TEXT,                -- Informatica XML pattern
    DBT_PATTERN TEXT,                 -- Equivalent DBT SQL
    DESCRIPTION TEXT,                 -- Searchable description
    TAGS ARRAY,
    COMPLEXITY VARCHAR(16),           -- LOW, MEDIUM, HIGH
    USE_CASE VARCHAR(128),
    EDGE_CASES TEXT,
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ
)
```

### Cortex Search Service

```sql
-- RAG search over corpus
CREATE CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH
ON DESCRIPTION
ATTRIBUTES TRANSFORMATION_TYPE, COMPLEXITY, USE_CASE
WAREHOUSE = COCOPROV_WH
TARGET_LAG = '1 hour'
AS SELECT 
    EXAMPLE_ID,
    TRANSFORMATION_TYPE,
    INFA_PATTERN,
    DBT_PATTERN,
    DESCRIPTION,
    COMPLEXITY,
    USE_CASE
FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS;
```

## RAG Corpus Search

Agent 2 queries the corpus before generating DBT code:

```python
def search_corpus(transformation_type: str, limit: int = 3) -> list:
    """Search for relevant conversion examples."""
    query = f"""
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
            '{{"query": "{transformation_type} conversion pattern",
              "columns": ["TRANSFORMATION_TYPE", "INFA_PATTERN", "DBT_PATTERN"],
              "filter": {{"@eq": {{"TRANSFORMATION_TYPE": "{transformation_type}"}}}},
              "limit": {limit}}}'
        )
    )['results']
    """
    return execute_sql(query)
```

### Adding Corpus Examples

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS 
(EXAMPLE_ID, TRANSFORMATION_TYPE, INFA_PATTERN, DBT_PATTERN, DESCRIPTION, TAGS, COMPLEXITY, USE_CASE)
SELECT 
    'EXP001', 
    'Expression',
    '<TRANSFORMATION TYPE="Expression"><TABLEATTRIBUTE EXPRESSION="qty * price"/></TRANSFORMATION>',
    'SELECT qty * price AS total FROM source_cte',
    'Convert arithmetic Expression to SQL calculation',
    ARRAY_CONSTRUCT('calculation', 'arithmetic'),
    'LOW',
    'Simple calculations';
```

## State Management

### Register Pipeline Run

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
(RUN_ID, WORKFLOW_NAME, STATUS, CURRENT_PHASE)
VALUES ('run_abc123', 'wf_m_TD_to_TD', 'IN_PROGRESS', 'CONVERSION');
```

### Update Pipeline Status

```sql
UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
SET STATUS = 'COMPLETED', 
    COMPLETED_AT = CURRENT_TIMESTAMP(),
    CURRENT_PHASE = 'DONE'
WHERE RUN_ID = 'run_abc123';
```

### Register Converted Model

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.MODEL_REGISTRY 
(MODEL_ID, RUN_ID, MODEL_NAME, SOURCE_WORKFLOW, SQL_CONTENT, TRANSFORMATION_TYPES)
SELECT 
    'mdl_xyz789',
    'run_abc123',
    'mart_tgt_td_table',
    'wf_m_TD_to_TD',
    $${{ config(materialized='table') }}
    WITH source AS (SELECT * FROM {{ source('raw', 'src_td_table') }})
    SELECT * FROM source$$,
    ARRAY_CONSTRUCT('Expression', 'Filter');
```

### Record Fidelity Score

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.FIDELITY_SCORES
(SCORE_ID, MODEL_ID, RUN_ID, OVERALL_SCORE, STRUCTURE_SCORE, SEMANTICS_SCORE)
VALUES ('scr_001', 'mdl_xyz789', 'run_abc123', 0.85, 0.90, 0.80);
```

### Query Pipeline Progress

```sql
-- Current run status
SELECT RUN_ID, WORKFLOW_NAME, STATUS, CURRENT_PHASE, 
       TIMESTAMPDIFF('minute', STARTED_AT, CURRENT_TIMESTAMP()) AS MINUTES_ELAPSED
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE
WHERE STATUS = 'IN_PROGRESS';

-- Model conversion summary
SELECT 
    ps.RUN_ID,
    COUNT(mr.MODEL_ID) AS MODELS_CONVERTED,
    AVG(fs.OVERALL_SCORE) AS AVG_FIDELITY,
    SUM(CASE WHEN fs.OVERALL_SCORE >= 0.8 THEN 1 ELSE 0 END) AS HIGH_QUALITY_COUNT
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE ps
LEFT JOIN INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr ON ps.RUN_ID = mr.RUN_ID
LEFT JOIN INFA2DBT_DB.PIPELINE.FIDELITY_SCORES fs ON mr.MODEL_ID = fs.MODEL_ID
GROUP BY ps.RUN_ID;

-- Models needing review (low fidelity)
SELECT mr.MODEL_NAME, fs.OVERALL_SCORE, mr.TRANSFORMATION_TYPES
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr
JOIN INFA2DBT_DB.PIPELINE.FIDELITY_SCORES fs ON mr.MODEL_ID = fs.MODEL_ID
WHERE fs.OVERALL_SCORE < 0.7
ORDER BY fs.OVERALL_SCORE ASC;
```

## Invocation Modes

### Mode 1: Full Pipeline
```
Run INFA2DBT accelerator on /path/to/workflows/
```

### Mode 2: Phase-Specific
```
Run INFA2DBT Phase 1 on /path/to/workflow.xml
Run INFA2DBT Phase 3 ROI analysis
Run INFA2DBT Phase 4-5 optimization for tier_1 models
```

### Mode 3: Single Agent
```
Run INFA2DBT Agent 1 on /path/to/workflow.xml
Run INFA2DBT Agent 3 on /path/to/model.sql
```

### Mode 4: Resume from Failure
```sql
-- Find failed run
SELECT RUN_ID, WORKFLOW_NAME, ERROR_MSG 
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
WHERE STATUS = 'FAILED';

-- Resume
Resume INFA2DBT run run_abc123
```

## Inputs

- **Required:** `workflow_xml_path` — Single XML or directory of XMLs
- **Required:** `dbt_project_path` — Target DBT project root
- **Required:** `coding_guidelines_path` — Version-locked coding guidelines
- **Optional:** `corpus_examples_dir` — Additional labelled examples (auto-ingested)
- **Optional:** `pipeline_config` — Custom thresholds and settings

## Pre-conditions

- Gate 0 (Pre-flight) complete:
  - `docs/corpus_coverage.csv` exists
  - `docs/scheduling_audit.xlsx` reviewed
  - `docs/INFA2DBT_coding_guidelines.md` finalized
- DBT project initialized
- Snowflake connection active with sandbox access
- INFA2DBT_DB.PIPELINE schema accessible

## Phase 1 Workflow (v2.0)

### Step 1: Initialize pipeline state in Snowflake

```python
run_id = str(uuid.uuid4())[:8]
execute_sql(f"""
    INSERT INTO INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
    (RUN_ID, WORKFLOW_NAME, STATUS, CURRENT_PHASE, STARTED_AT)
    VALUES ('{run_id}', 'BATCH_{batch_name}', 'IN_PROGRESS', 'CONVERSION', CURRENT_TIMESTAMP())
""")
```

### Step 2: Process workflows with RAG-enhanced conversion

```python
for workflow in workflows:
    # Agent 1: Parse and decompose
    handoffs = invoke_skill('infa-xml-parser', {
        'workflow_xml_path': workflow,
        'corpus_coverage_csv': corpus_coverage_path
    })
    
    for handoff in handoffs:
        # Search corpus for relevant patterns
        transformation_types = handoff['transformation_types']
        corpus_examples = {}
        for t_type in transformation_types:
            examples = search_corpus(t_type, limit=3)
            if examples:
                corpus_examples[t_type] = examples[0]
        
        # Agent 2: Convert to DBT (RAG-enhanced)
        conversion = invoke_skill('infa-to-dbt-converter', {
            'handoff_json_path': handoff['path'],
            'coding_guidelines_path': coding_guidelines_path,
            'corpus_examples': corpus_examples  # NEW: RAG context
        })
        
        # Register model in Snowflake
        model_id = register_model(run_id, conversion)
        
        # Agent 3: Score fidelity
        fidelity = invoke_skill('conversion-fidelity-scorer', {
            'model_sql_path': conversion['model_path'],
            'agent2_output_json': conversion,
            'handoff_json_path': handoff['path']
        })
        
        # Record fidelity score in Snowflake
        record_fidelity_score(model_id, run_id, fidelity['scores'])
        
        # Continue with Agent 4, 5...
```

### Step 3: Update pipeline status on completion

```python
execute_sql(f"""
    UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
    SET STATUS = 'COMPLETED', 
        COMPLETED_AT = CURRENT_TIMESTAMP(),
        CURRENT_PHASE = 'DONE'
    WHERE RUN_ID = '{run_id}'
""")
```

## Error Recovery

### Resume from Snowflake state

```sql
-- Find incomplete run
SELECT RUN_ID, WORKFLOW_NAME, CURRENT_PHASE, ERROR_MSG
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE
WHERE STATUS IN ('IN_PROGRESS', 'FAILED')
ORDER BY STARTED_AT DESC
LIMIT 1;

-- Find unprocessed models from run
SELECT mr.MODEL_NAME, mr.FIDELITY_STATUS
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr
WHERE mr.RUN_ID = '{run_id}' 
  AND mr.FIDELITY_STATUS = 'PENDING';
```

### Retry exhaustion handling

```python
if retry_count >= max_retries:
    execute_sql(f"""
        UPDATE INFA2DBT_DB.PIPELINE.MODEL_REGISTRY
        SET FIDELITY_STATUS = 'QUARANTINED'
        WHERE MODEL_ID = '{model_id}'
    """)
```

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
| 0 | Pre-flight | 🟡 | Corpus + Scheduling audit |
| 1 | Phase 1 | 🟡 | Engineering Lead sign-off |
| 2 | Phase 2 | 🟡 | Stabilization completion |
| 3 | Phase 3 | 🟡 | ROI tier confirmation |
| 4 | Phase 4 | 🟡 | Config changes validation |
| 5 | Phase 5 | 🔴 | Each Pattern C rewrite |

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

## Example Invocation

```
User: Convert all Informatica workflows in /mnt/data/infa_exports/ to DBT

Orchestrator v2.0:
1. Register pipeline run in PIPELINE_STATE
2. Verify pre-flight complete (Gate 0)
3. Enumerate 47 workflow XMLs
4. For each workflow:
   a. Parse with Agent 1
   b. Search corpus for transformation patterns
   c. Convert with Agent 2 (RAG-enhanced)
   d. Register model in MODEL_REGISTRY
   e. Score with Agent 3
   f. Record fidelity in FIDELITY_SCORES
   g. Validate with Agent 4
5. Generate Gate 1 sign-off package
6. Update PIPELINE_STATE to COMPLETED
7. Await human approval
```

## Monitoring Queries

```sql
-- Active pipeline runs
SELECT * FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE WHERE STATUS = 'IN_PROGRESS';

-- Conversion success rate
SELECT 
    COUNT(*) AS total_models,
    AVG(fs.OVERALL_SCORE) AS avg_fidelity,
    SUM(CASE WHEN fs.OVERALL_SCORE >= 0.8 THEN 1 ELSE 0 END) AS high_quality
FROM INFA2DBT_DB.PIPELINE.FIDELITY_SCORES fs;

-- Most common transformation types
SELECT 
    t.VALUE::STRING AS transformation_type,
    COUNT(*) AS count
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr,
     LATERAL FLATTEN(mr.TRANSFORMATION_TYPES) t
GROUP BY 1 ORDER BY 2 DESC;

-- Corpus coverage gaps
SELECT DISTINCT t.VALUE::STRING AS transformation_type
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr,
     LATERAL FLATTEN(mr.TRANSFORMATION_TYPES) t
WHERE t.VALUE::STRING NOT IN (
    SELECT DISTINCT TRANSFORMATION_TYPE FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS
);
```
