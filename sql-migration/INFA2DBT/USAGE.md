# INFA2DBT Accelerator — Usage Guide

## Quick Start

### Prerequisites

1. **Snowflake Access**: Active connection with appropriate privileges
2. **Cortex Code**: CLI installed and configured
3. **INFA2DBT Objects**: Database, tables, and Cortex Search service created (see MIGRATION.md)

### Basic Usage

```bash
# Convert a single Informatica XML workflow
Run INFA2DBT accelerator on /path/to/workflow.xml

# Convert all XMLs in a directory
Run INFA2DBT accelerator on /path/to/xml_directory/

# Resume a failed pipeline run
Resume INFA2DBT run <run_id>
```

---

## Input Methods

### Option 1: Local Files (Recommended for Development)

Place Informatica PowerCenter XML exports in a local directory:

```
/path/to/xml_files/
├── wf_m_customer_load.xml
├── wf_m_product_sync.xml
└── wf_m_sales_aggregate.xml
```

Then run:
```
Run INFA2DBT Phase 1 on /path/to/xml_files/
```

### Option 2: Snowflake Stage (Recommended for Production)

```sql
-- Create a stage for XML files
CREATE STAGE IF NOT EXISTS INFA2DBT_DB.PIPELINE.INFA_XMLS;

-- Upload XML files
PUT file:///path/to/workflow.xml @INFA2DBT_DB.PIPELINE.INFA_XMLS;

-- List uploaded files
LIST @INFA2DBT_DB.PIPELINE.INFA_XMLS;
```

Then reference the stage:
```
Run INFA2DBT accelerator on @INFA2DBT_DB.PIPELINE.INFA_XMLS/workflow.xml
```

### Option 3: Direct XML Content

For small workflows, paste XML content directly:
```
Convert this Informatica mapping to DBT:
<XML content here>
```

---

## Invocation Modes

### Mode 1: Full Pipeline

Runs all 5 phases with 6 human checkpoints:

```
Run INFA2DBT accelerator on /path/to/workflows/
```

**Output:**
- DBT models in `models/converted/`
- Schema YAML files
- Unit test YAML files
- Pipeline state in `INFA2DBT_DB.PIPELINE.PIPELINE_STATE`

### Mode 2: Phase-Specific

Run individual phases:

```bash
# Phase 1: Conversion only
Run INFA2DBT Phase 1 on /path/to/workflow.xml

# Phase 3: ROI analysis (after Phase 2 stabilization)
Run INFA2DBT Phase 3 ROI analysis

# Phase 4-5: Optimization for high-ROI models
Run INFA2DBT Phase 4-5 optimization for tier_1 models
```

### Mode 3: Single Agent

Invoke individual agents for specific tasks:

```bash
# Agent 1: Parse XML only
Run INFA2DBT Agent 1 on /path/to/workflow.xml

# Agent 2: Convert handoff to DBT
Run INFA2DBT Agent 2 on /path/to/handoff.json

# Agent 3: Score conversion fidelity
Run INFA2DBT Agent 3 on /path/to/model.sql
```

### Mode 4: Reprocess Quarantined Models

```
Reprocess INFA2DBT quarantined model /path/to/model.sql
```

---

## Adding Corpus Examples (RAG)

The accelerator uses Cortex Search to find relevant conversion patterns. Add examples to improve conversion quality.

### View Current Corpus

```sql
SELECT TRANSFORMATION_TYPE, COUNT(*) AS EXAMPLES
FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS
GROUP BY TRANSFORMATION_TYPE
ORDER BY 2 DESC;
```

### Add New Example

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS 
(EXAMPLE_ID, TRANSFORMATION_TYPE, INFA_PATTERN, DBT_PATTERN, DESCRIPTION, TAGS, COMPLEXITY, USE_CASE)
SELECT 
    'CUSTOM001',
    'Expression',
    '<TRANSFORMATION TYPE="Expression"><TABLEATTRIBUTE EXPRESSION="NVL(amount, 0)"/></TRANSFORMATION>',
    'SELECT COALESCE(amount, 0) AS amount FROM source_cte',
    'Convert NVL null-handling to COALESCE in Snowflake',
    ARRAY_CONSTRUCT('null_handling', 'nvl', 'coalesce'),
    'LOW',
    'Null value replacement';
```

### Bulk Load Examples from CSV

```sql
-- Create file format
CREATE OR REPLACE FILE FORMAT INFA2DBT_DB.PIPELINE.CORPUS_CSV
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- Upload and load
PUT file:///path/to/corpus_examples.csv @INFA2DBT_DB.PIPELINE.INFA_XMLS;

COPY INTO INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS
FROM @INFA2DBT_DB.PIPELINE.INFA_XMLS/corpus_examples.csv
FILE_FORMAT = INFA2DBT_DB.PIPELINE.CORPUS_CSV;
```

### Search Corpus

```sql
-- Test semantic search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
        '{"query": "aggregation sum group by", "columns": ["TRANSFORMATION_TYPE", "DBT_PATTERN", "DESCRIPTION"], "limit": 5}'
    )
)['results'] AS results;
```

---

## Monitoring Pipeline Progress

### Active Runs

```sql
SELECT RUN_ID, WORKFLOW_NAME, STATUS, CURRENT_PHASE,
       TIMESTAMPDIFF('minute', STARTED_AT, CURRENT_TIMESTAMP()) AS MINUTES_ELAPSED
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE
WHERE STATUS = 'IN_PROGRESS'
ORDER BY STARTED_AT DESC;
```

### Conversion Summary

```sql
SELECT 
    ps.RUN_ID,
    ps.WORKFLOW_NAME,
    COUNT(mr.MODEL_ID) AS MODELS_CONVERTED,
    AVG(fs.OVERALL_SCORE) AS AVG_FIDELITY,
    SUM(CASE WHEN fs.OVERALL_SCORE >= 0.8 THEN 1 ELSE 0 END) AS HIGH_QUALITY
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE ps
LEFT JOIN INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr ON ps.RUN_ID = mr.RUN_ID
LEFT JOIN INFA2DBT_DB.PIPELINE.FIDELITY_SCORES fs ON mr.MODEL_ID = fs.MODEL_ID
GROUP BY ps.RUN_ID, ps.WORKFLOW_NAME
ORDER BY ps.STARTED_AT DESC;
```

### Models Needing Review (Low Fidelity)

```sql
SELECT mr.MODEL_NAME, fs.OVERALL_SCORE, mr.TRANSFORMATION_TYPES
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr
JOIN INFA2DBT_DB.PIPELINE.FIDELITY_SCORES fs ON mr.MODEL_ID = fs.MODEL_ID
WHERE fs.OVERALL_SCORE < 0.7
ORDER BY fs.OVERALL_SCORE ASC;
```

### Transformation Type Coverage

```sql
-- Most common transformations
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

---

## Resume from Failure

### Find Failed Run

```sql
SELECT RUN_ID, WORKFLOW_NAME, ERROR_MSG, CURRENT_PHASE
FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE
WHERE STATUS = 'FAILED'
ORDER BY STARTED_AT DESC
LIMIT 5;
```

### Resume Pipeline

```
Resume INFA2DBT run <run_id>
```

### Find Unprocessed Models

```sql
SELECT mr.MODEL_NAME, mr.FIDELITY_STATUS
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY mr
WHERE mr.RUN_ID = '<run_id>' 
  AND mr.FIDELITY_STATUS = 'PENDING';
```

---

## Output Locations

| Artifact | Location | Description |
|----------|----------|-------------|
| DBT Models | `models/converted/*.sql` | Generated SQL models |
| Schema YAML | `models/converted/*.schema.yml` | Column docs + tests |
| Unit Tests | `tests/unit/*_unit.yml` | DBT native unit tests |
| Agent Logs | `logs/agent{N}/*.json` | Per-agent execution logs |
| Handoffs | `handoffs/*_handoff.json` | Agent 1→2 intermediate state |
| Pipeline State | `INFA2DBT_DB.PIPELINE.PIPELINE_STATE` | Run tracking |
| Model Registry | `INFA2DBT_DB.PIPELINE.MODEL_REGISTRY` | Converted models + SQL |
| Fidelity Scores | `INFA2DBT_DB.PIPELINE.FIDELITY_SCORES` | Quality metrics |

---

## Common Commands

```bash
# Check current pipeline state
SELECT * FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE ORDER BY STARTED_AT DESC LIMIT 5;

# View model SQL from registry
SELECT MODEL_NAME, SQL_CONTENT 
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY 
WHERE MODEL_NAME = 'mart_customer_summary';

# Export model to file
SELECT SQL_CONTENT FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY WHERE MODEL_NAME = 'x';

# Clear failed runs (use with caution)
UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
SET STATUS = 'CANCELLED' 
WHERE STATUS = 'FAILED' AND RUN_ID = '<run_id>';
```

---

## Troubleshooting

### "Corpus search warning" errors
- Verify Cortex Search service is ACTIVE: `SHOW CORTEX SEARCH SERVICES IN INFA2DBT_DB.PIPELINE`
- Check warehouse is running: `ALTER WAREHOUSE COCOPROV_WH RESUME`

### Low fidelity scores
- Add more corpus examples for the transformation types in question
- Review the specific transformations flagged in `FLAGGED_ISSUES` array

### XML parsing failures
- Ensure XML declaration is on line 1 (no comments before `<?xml`)
- Validate XML is well-formed: `xmllint --noout workflow.xml`

### Model registry not updating
- Check Snowflake connection permissions
- Verify INSERT privilege on `MODEL_REGISTRY` table
