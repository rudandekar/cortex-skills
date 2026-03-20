# INFA2DBT Accelerator — Usage Guide

## Quick Start

### Prerequisites

1. **Snowflake Access**: Active connection with appropriate privileges
2. **Python 3.9+**: For running Agent 1 and Agent 2 scripts
3. **INFA2DBT Objects**: Database, tables, and Cortex Search service created (see MIGRATION.md)

### Command Line Usage

```bash
# Step 1: Parse Informatica XML files
python3 src/agent1_parser.py --xml-dir ./xml_exports --output-dir ./artifacts/handoffs

# Step 2: Convert handoffs to dbt models
python3 src/agent2_converter_v2.py --handoff-dir ./artifacts/handoffs --output-dir ./artifacts/output

# Without Snowflake state persistence (offline mode)
python3 src/agent2_converter_v2.py --handoff-dir ./artifacts/handoffs --output-dir ./artifacts/output --no-state

# Offline mode via environment variable (bypasses Snowflake connection entirely)
INFA2DBT_NO_STATE=1 python3 src/agent2_converter_v2.py --handoff-dir ./artifacts/handoffs --output-dir ./artifacts/output --no-state

# Step 3: Run dbt models
cd artifacts/output && dbt run
```

### Model Naming Convention (v2.1)

dbt model names follow the format:

```
{xml_filename}_{seq:02d}_{stg|int|mart}_{target_name}
```

| Component | Source | Example |
|-----------|--------|---------|
| `xml_filename` | Informatica XML filename (lowercased, sanitized) | `edwtd_gl_wf_ff_cap_accounts` |
| `seq` | Zero-padded target order within the XML | `01`, `02`, `03` |
| `stg\|int\|mart` | Layer prefix based on target name heuristics | `mart_` |
| `target_name` | Target table name (lowercased) | `cap_accounts` |

**Full example:** `edwtd_gl_wf_ff_cap_accounts_01_mart_cap_accounts.sql`

This convention prevents overwrites when multiple Informatica workflows write
to the same target table, as the XML filename prefix is unique per workflow.

### Cortex Code Usage

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

### Consolidation Candidates (Duplicate Targets)

When multiple Informatica workflows write to the same target table, Agent 2 logs
a warning during conversion. Use this query to review all consolidation opportunities:

```sql
-- Find all target tables with multiple models
SELECT 
    TARGET_TABLE,
    TARGET_SCHEMA,
    COUNT(DISTINCT MODEL_NAME) AS model_count,
    ARRAY_AGG(DISTINCT MODEL_NAME) AS models,
    ARRAY_AGG(DISTINCT SOURCE_WORKFLOW) AS source_workflows
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY
GROUP BY TARGET_TABLE, TARGET_SCHEMA
HAVING COUNT(DISTINCT MODEL_NAME) > 1
ORDER BY model_count DESC;
```

**Example output during conversion:**
```
Converting: mart_customer_web
  [WARN] Target 'ANALYTICS.MART_CUSTOMER_360' already has 2 model(s):
         - mart_customer_crm (from wf_crm_load)
         - mart_customer_legacy (from wf_legacy_migration)
         → Consolidation opportunity detected. Review in Phase 3 (Agent 6).
```

For deep analysis with SQL similarity scoring, run Agent 6 (Phase 3) after stabilization.

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
- Check warehouse is running: `ALTER WAREHOUSE INFA2DBT_WH RESUME`

### Low fidelity scores
- Add more corpus examples for the transformation types in question
- Review the specific transformations flagged in `FLAGGED_ISSUES` array

### XML parsing failures
- Ensure XML declaration is on line 1 (no comments before `<?xml`)
- Validate XML is well-formed: `xmllint --noout workflow.xml`

### Model registry not updating
- Check Snowflake connection permissions
- Verify INSERT privilege on `MODEL_REGISTRY` table

### Snowflake connector TOML parse error
If you see `tomlkit.exceptions.EmptyKeyError` when running Agent 2:
- **Root cause:** Malformed `~/.snowflake/connections.toml` (e.g., empty key at a specific line)
- **Workaround:** Run in offline mode with the environment variable:
  ```bash
  INFA2DBT_NO_STATE=1 python3 src/agent2_converter_v2.py --handoff-dir ./artifacts/handoffs --output-dir ./artifacts/output --no-state
  ```
- **Permanent fix:** Edit `~/.snowflake/connections.toml` and remove or fix the malformed line
- Note: Offline mode skips MODEL_REGISTRY, FIDELITY_SCORES, and corpus search

### Model name collisions / overwrites
If fewer `.sql` files are generated than expected handoffs:
- **Root cause (v2.0):** Multiple Informatica workflows targeting the same table produced identical model names
- **Fix:** Upgrade to v2.1+ which prefixes model names with the XML filename (guaranteed unique)
- **Verify:** `ls models/converted/*.sql | wc -l` should match the handoff count

---

## New in v2.2

### Batch dbt Model Generation (agent2_batch.py)

Convert multiple handoffs in batch with tier-based classification:

```bash
# Batch convert all handoffs in a directory
python3 agents/infa-to-dbt-converter/scripts/agent2_batch.py \
  --handoff-dir ./artifacts/handoffs \
  --output-dir ./artifacts/output

# Convert only Tier 1 (high-complexity) handoffs
python3 agents/infa-to-dbt-converter/scripts/agent2_batch.py \
  --handoff-dir ./artifacts/handoffs \
  --output-dir ./artifacts/output \
  --tier 1

# Convert Tier 2 (medium-complexity) handoffs
python3 agents/infa-to-dbt-converter/scripts/agent2_batch.py \
  --handoff-dir ./artifacts/handoffs \
  --output-dir ./artifacts/output \
  --tier 2
```

**Tier classification:**

| Tier | Complexity | Description |
|------|-----------|-------------|
| 1 | High | Multi-target, complex joins, Teradata dialect |
| 2 | Medium | Standard transformations, moderate logic |
| 3 | Low | Simple source-to-target, passthrough mappings |

**Dialect conversion features:**
- Teradata-to-Snowflake SQL dialect conversion
- Post SQL `UPDATE` → `MERGE` statement rewriting
- `$$param` → `{{ var() }}` dbt parameter conversion

### PowerCenterParser v3.0 Batch Mode (parse_pc_xml.py)

Parse large volumes of Informatica XML with the rewritten OOP parser:

```bash
# Parse a directory of XMLs with v3.0 parser
python3 agents/infa-xml-parser/scripts/parse_pc_xml.py \
  --xml-dir ./xml_exports \
  --output-dir ./artifacts/handoffs \
  --batch

# Parse with verbose logging
python3 agents/infa-xml-parser/scripts/parse_pc_xml.py \
  --xml-dir ./xml_exports \
  --output-dir ./artifacts/handoffs \
  --batch --verbose
```

**v3.0 parser improvements:**
- TYPE_ALIASES normalization (`Lookup` → `Lookup Procedure`, `Sequence Generator` → `Sequence`)
- INSTANCE extraction for transformation reuse tracking
- 20+ XML element types supported
- Malformed XML sanitizer for production exports with encoding issues

### Source/Target CSV Extraction (Agent 8)

Extract source-to-target lineage from Informatica XMLs for impact analysis:

```bash
# Extract source/target mappings to CSV
python3 agents/informatica-source-target/scripts/extract_source_target.py \
  --xml-dir ./xml_exports \
  --output ./artifacts/source_target_mapping.csv

# Extract for a single workflow
python3 agents/informatica-source-target/scripts/extract_source_target.py \
  --xml-file ./xml_exports/wf_m_customer_load.xml \
  --output ./artifacts/customer_load_mapping.csv
```

**Output CSV columns:** workflow_name, mapping_name, source_table, source_schema, target_table, target_schema, transformation_count

### Gap Analysis Reference

See [GAPS.md](GAPS.md) for the comprehensive parser gap analysis from scanning 34,503 production XMLs. The document includes:
- P0–P3 prioritized remediation plan
- Transformation type coverage matrix
- Known unsupported XML patterns
