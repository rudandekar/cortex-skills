# INFA2DBT Accelerator — Technical Documentation

## Overview

The INFA2DBT Accelerator is an AI-powered pipeline that converts Informatica PowerCenter workflows to production-ready DBT models on Snowflake. It uses a multi-agent architecture with RAG-enhanced code generation and persistent state management.

**Version:** 2.2.0  
**Author:** Deloitte FDE Practice  
**Platform:** Snowflake + Cortex Code

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    INFA2DBT ACCELERATOR v2.2                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  Agent 1    │───▶│  Agent 2    │───▶│  Agent 3    │───▶│  Agent 4    │  │
│  │ XML Parser  │    │ Converter   │    │  Fidelity   │    │ Validation  │  │
│  │ (v3.0 OOP)  │    │ (+Batch)    │    │             │    │             │  │
│  └──────┬──────┘    └──────┬──────┘    └─────────────┘    └─────────────┘  │
│         │                  │                                                │
│         ▼                  ▼                                                │
│  ┌─────────────┐  ┌────────────────┐                                       │
│  │  Agent 8    │  │ Cortex Search  │  RAG Corpus                           │
│  │ Src-Target  │  │    Service     │  (12+ examples)                       │
│  │  Extractor  │  └────────────────┘                                       │
│  └─────────────┘                                                           │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│                        SNOWFLAKE STATE STORE                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ PIPELINE_STATE  │  │ MODEL_REGISTRY  │  │ FIDELITY_SCORES │             │
│  │   (Run tracking)│  │  (SQL + YAML)   │  │ (Quality metrics)│             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │  Agent 5    │───▶│  Agent 6    │───▶│  Agent 7    │                     │
│  │  Handoff    │    │ ROI Scorer  │    │ Optimizer   │                     │
│  └─────────────┘    └─────────────┘    └─────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Agent Descriptions

### Agent 1: XML Parser (`infa-xml-parser`)
**Purpose:** Parse Informatica PowerCenter XML exports and decompose into handoff objects.

**Inputs:**
- Informatica workflow XML file(s)
- Corpus coverage CSV (optional)

**Outputs:**
- Handoff JSON files (one per target table)
- Source/target metadata extraction
- Transformation chain identification

**Key Features:**
- Handles nested mappings and mapplets
- Extracts SQL overrides from Source Qualifiers
- Identifies transformation types for corpus matching
- Collision-free model naming using XML filename prefix + sequence number (v2.1)

**PowerCenterParser v3.0** (`agents/infa-xml-parser/scripts/parse_pc_xml.py`):
- Complete OOP rewrite — `PowerCenterParser` class with `parse()` entry point
- `TYPE_ALIASES` normalization (`Lookup` → `Lookup Procedure`, `Sequence Generator` → `Sequence`)
- `INSTANCE` element extraction with `instance_map` for connector graph accuracy
- 20+ previously-unhandled XML elements: `WORKLET`, `SHORTCUT`, `FLATFILE`, `ASSOCIATED_SOURCE_INSTANCE`, `MAPPINGVARIABLE`, `WORKFLOWVARIABLE`, `TARGETLOADORDER`, `CONFIG`, `PARTITION`, `SESSIONCOMPONENT`, `SCHEDULER`, `GROUP`, `SESSTRANSFORMATIONINST`, `ERPINFO`, `METADATAEXTENSION`, `FIELDDEPENDENCY`
- Malformed XML sanitizer with automatic re-parse (handles unescaped ampersands and attribute quote mismatches)
- Gap remediation from corpus scan of 34,503 XMLs (see `GAPS.md`)

---

### Agent 2: Code Converter (`infa-to-dbt-converter`)
**Purpose:** Generate DBT SQL models from handoff objects, enhanced with RAG corpus search.

**Inputs:**
- Handoff JSON from Agent 1
- Coding guidelines path
- Corpus examples (via Cortex Search)

**Outputs:**
- DBT model SQL file
- Schema YAML file
- Unit test YAML file
- Model registration in Snowflake

**Key Features (v2.1):**
- **RAG Integration:** Queries `INFA2DBT_CORPUS_SEARCH` for relevant patterns
- **Persistent State:** Registers models in `MODEL_REGISTRY` table
- **Fidelity Recording:** Calculates and stores quality scores
- **Duplicate Target Detection:** Real-time warning when target table already has models
- **Security Hardening:** Parameterized queries, JSON injection prevention
- **Offline Mode:** `--no-state` flag or `INFA2DBT_NO_STATE=1` env var bypasses Snowflake
- **Substring Materialization Matching:** Detects `stg_`/`int_`/`mart_` anywhere in model name (not just prefix)

**Batch Converter** (`agents/infa-to-dbt-converter/scripts/agent2_batch.py`):
- Programmatic batch model generator — reads handoff JSONs and outputs `.sql`, `.schema.yml`, `_unit.yml`
- Tier classification: Tier 1 (single SQ, 0-1 transforms), Tier 2 (2-3 standard), Tier 3 (4+ or escalate)
- Teradata-to-Snowflake SQL dialect conversion (`USER` → `CURRENT_USER()`, `NVL` → `COALESCE`, `CURRENT_TIMESTAMP(0)` → `CURRENT_TIMESTAMP()`)
- Post SQL `UPDATE...FROM` → Snowflake `MERGE INTO` conversion
- `$$param` → `{{ var('param') }}` parameter reference conversion
- Primary key inference from field naming patterns (`BK_`, `_KEY`, `_ID`, `_PK`)
- `--tier` CLI filter to process only specific tiers
- Summary CSV and per-model conversion log output

**Transformation Support:**

| Type | Status | DBT Pattern |
|------|--------|-------------|
| Source Qualifier | ✅ Full | `source()` macro with CTE |
| Expression | ✅ Full | SELECT with calculated columns |
| Filter | ✅ Full | WHERE clause |
| Aggregator | ✅ Full | GROUP BY with aggregate functions |
| Joiner | ✅ Full | JOIN (INNER/LEFT/RIGHT/FULL) |
| Lookup (connected) | ✅ Full | LEFT JOIN CTE |
| Lookup (unconnected) | ✅ Full | Scalar subquery |
| Sequence Generator | ✅ Full | ROW_NUMBER() |
| Sorter | ✅ Full | ORDER BY |
| Router | ✅ Full | Multiple models with WHERE |
| Union | ✅ Full | UNION ALL |
| Normalizer | ✅ Full | LATERAL FLATTEN |
| Rank | ✅ Full | Window functions |
| Update Strategy | ✅ Full | Incremental materialization |

---

### Agent 3: Fidelity Scorer (`conversion-fidelity-scorer`)
**Purpose:** Score conversion correctness and semantic equivalence.

**Metrics Calculated:**
- **Overall Score:** Weighted average (0.0-1.0)
- **Structure Score:** CTE count vs transformation count
- **Semantics Score:** Logic preservation validation
- **Test Coverage:** Unit test generation completeness
- **Transformation Coverage:** % of transformations converted

**Decisions:**
- `pass` → Continue to Agent 4
- `retry` → Send back to Agent 2 (max 3 attempts)
- `quarantine` → Route to review queue

---

### Agent 4: Validation Critique (`dbt-validation-critique`)
**Purpose:** Validate DBT model compliance with coding guidelines.

**Checks:**
- Naming conventions (stg_, int_, mart_ prefixes)
- Config block completeness
- Tag presence (workflow, frequency, domain)
- Schema YAML documentation
- Unit test coverage

---

### Agent 5: Phase Handoff (`phase1-handoff`)
**Purpose:** Manage checkpoints and human gates.

**Checkpoint Types:**
- **Checkpoint A:** Per-workflow completion
- **Checkpoint B:** Batch milestone (every N workflows)
- **Checkpoint C:** Phase 1 completion (Gate 1)

---

### Agent 6: ROI Scorer (`roi-subgraph-scorer`)
**Purpose:** Analyze query patterns, tier models by optimization ROI, and detect consolidation opportunities.

**Tiers:**
- **Tier 1:** High-ROI, high-frequency queries → Optimize first
- **Tier 2:** Medium-ROI → Optimize if time permits
- **Tier 3:** Low-ROI → Accept as-is

**Target Consolidation Analysis (v2.0):**
Detects multiple models targeting the same table and recommends consolidation:

| Similarity | Priority | Recommendation |
|------------|----------|----------------|
| >70% | HIGH | MERGE - Models are near-duplicates |
| 40-70% | MEDIUM | REVIEW - Significant overlap |
| <40% | LOW | KEEP SEPARATE - Distinct logic |

**Similarity Algorithm:** Jaccard index on CTE structure (names + logic hashes)

---

### Agent 7: SQL Optimizer (`dbt-sql-optimizer`)
**Purpose:** Performance optimization in two sub-phases.

**7A: Config Optimization**
- Clustering keys
- Search optimization
- Materialization tuning

**7B: SQL Optimization**
- Query rewrites
- Predicate pushdown
- Join optimization

---

### Agent 8: Source-Target Extractor (`informatica-source-target`)
**Purpose:** Extract source and target table mappings from PowerCenter XML into flat CSV for impact analysis, lineage audits, and migration planning.

**Inputs:**
- `--xml-dir <path>` or `--xml-file <path>` — PowerCenter XML export(s)
- `--output <path>` — output CSV path

**Outputs:**
- CSV file with columns: `workflow_name`, `type` (SOURCE/TARGET), `table_name`

**Key Features:**
- Reuses `PowerCenterParser` from `infa-xml-parser` agent
- Automatic deduplication of source/target pairs
- Batch processing of entire XML directories

---

## Gap Analysis (GAPS.md)

A comprehensive gap analysis was performed on 2026-03-18 by scanning 34,503 production XMLs. Key findings:

- **Transformation type mismatches:** `Lookup` (1,773 instances) and `Sequence Generator` (492) used different TYPE strings than the parser expected — fixed in v3.0 with `TYPE_ALIASES`
- **20+ unhandled XML elements** identified across Critical/High/Medium/Low severity tiers
- **Database type coverage:** Teradata is the dominant source (80,042 instances); Flat File at 15,837 files (45.9%)
- **50,337 Source Qualifier SQL overrides** require Teradata-dialect-aware parsing
- **Prioritized P0–P3 remediation plan** with 20 items — P0/P1 items addressed in PowerCenterParser v3.0

See `GAPS.md` for the full analysis.

---

## Snowflake Objects

### Database Structure

```
INFA2DBT_DB
└── PIPELINE
    ├── PIPELINE_STATE        (Table)
    ├── MODEL_REGISTRY        (Table)
    ├── FIDELITY_SCORES       (Table)
    ├── INFA2DBT_CORPUS       (Table)
    ├── INFA2DBT_CORPUS_SEARCH (Cortex Search Service)
    └── INFA_XMLS             (Stage - optional)
```

### Table Schemas

#### PIPELINE_STATE
Tracks pipeline run status and progress.

| Column | Type | Description |
|--------|------|-------------|
| RUN_ID | VARCHAR(64) | Primary key |
| WORKFLOW_NAME | VARCHAR(256) | Source workflow name |
| XML_FILE_PATH | VARCHAR(1024) | Source file location |
| STATUS | VARCHAR(32) | PENDING, IN_PROGRESS, COMPLETED, FAILED |
| CURRENT_PHASE | VARCHAR(64) | Current pipeline phase |
| CURRENT_GATE | VARCHAR(32) | Current human gate |
| STARTED_AT | TIMESTAMP_NTZ | Run start time |
| COMPLETED_AT | TIMESTAMP_NTZ | Run completion time |
| ERROR_MSG | TEXT | Error details if failed |
| RETRY_COUNT | INTEGER | Number of retries |
| METADATA | VARIANT | Additional run metadata |
| CREATED_BY | VARCHAR(128) | User who initiated |
| UPDATED_AT | TIMESTAMP_NTZ | Last update time |

#### MODEL_REGISTRY
Stores all converted DBT models with full SQL content.

| Column | Type | Description |
|--------|------|-------------|
| MODEL_ID | VARCHAR(64) | Primary key |
| RUN_ID | VARCHAR(64) | Foreign key to PIPELINE_STATE |
| MODEL_NAME | VARCHAR(256) | DBT model name |
| SOURCE_WORKFLOW | VARCHAR(256) | Informatica workflow |
| SOURCE_MAPPING | VARCHAR(256) | Informatica mapping |
| TARGET_TABLE | VARCHAR(256) | Target table name |
| TARGET_SCHEMA | VARCHAR(256) | Target schema |
| SQL_CONTENT | TEXT | Full DBT SQL |
| SCHEMA_YML | TEXT | Schema YAML content |
| UNIT_TEST_YML | TEXT | Unit test YAML content |
| TRANSFORMATION_TYPES | ARRAY | List of transformation types |
| COLUMN_COUNT | INTEGER | Number of output columns |
| CTE_COUNT | INTEGER | Number of CTEs |
| FIDELITY_STATUS | VARCHAR(32) | PENDING, PASSED, QUARANTINED |
| CREATED_AT | TIMESTAMP_NTZ | Creation timestamp |
| UPDATED_AT | TIMESTAMP_NTZ | Last update timestamp |

#### FIDELITY_SCORES
Tracks conversion quality metrics per model.

| Column | Type | Description |
|--------|------|-------------|
| SCORE_ID | VARCHAR(64) | Primary key |
| MODEL_ID | VARCHAR(64) | Foreign key to MODEL_REGISTRY |
| RUN_ID | VARCHAR(64) | Foreign key to PIPELINE_STATE |
| OVERALL_SCORE | FLOAT | Weighted average (0.0-1.0) |
| STRUCTURE_SCORE | FLOAT | CTE structure quality |
| SEMANTICS_SCORE | FLOAT | Logic preservation |
| TEST_COVERAGE | FLOAT | Unit test coverage |
| TRANSFORMATION_COVERAGE | FLOAT | % transformations converted |
| COLUMN_MAPPING_ACCURACY | FLOAT | Column mapping quality |
| LINEAGE_PRESERVED | BOOLEAN | Data lineage intact |
| FLAGGED_ISSUES | ARRAY | List of issues found |
| REVIEWER_NOTES | TEXT | Human reviewer notes |
| REVIEWED_BY | VARCHAR(128) | Reviewer name |
| REVIEWED_AT | TIMESTAMP_NTZ | Review timestamp |
| CREATED_AT | TIMESTAMP_NTZ | Score creation time |

#### INFA2DBT_CORPUS
RAG corpus with labelled conversion examples.

| Column | Type | Description |
|--------|------|-------------|
| EXAMPLE_ID | VARCHAR(64) | Primary key |
| TRANSFORMATION_TYPE | VARCHAR(64) | Expression, Filter, etc. |
| INFA_PATTERN | TEXT | Informatica XML pattern |
| DBT_PATTERN | TEXT | Equivalent DBT SQL |
| DESCRIPTION | TEXT | Searchable description |
| TAGS | ARRAY | Keywords for filtering |
| COMPLEXITY | VARCHAR(16) | LOW, MEDIUM, HIGH |
| USE_CASE | VARCHAR(128) | Business use case |
| EDGE_CASES | TEXT | Known edge cases |
| CREATED_AT | TIMESTAMP_NTZ | Creation time |
| UPDATED_AT | TIMESTAMP_NTZ | Last update time |

---

## Cortex Search Integration

### Service Definition

```sql
CREATE CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH
ON DESCRIPTION
ATTRIBUTES TRANSFORMATION_TYPE, COMPLEXITY, USE_CASE
WAREHOUSE = INFA2DBT_WH
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

### Query Pattern

Agent 2 uses this pattern to retrieve relevant examples:

```python
def search_corpus(transformation_type: str, limit: int = 3) -> list:
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

---

## Pipeline Phases

### Phase 1: Conversion (Automated)
1. **Gate 0 (Pre-flight):** Verify prerequisites
2. **Agent 1:** Parse XML → Handoff JSON (with collision-free naming)
3. **Agent 2:** Convert to DBT (RAG-enhanced, supports offline mode)
4. **Agent 3:** Score fidelity
5. **Agent 4:** Validate compliance
6. **Agent 5:** Checkpoint management
7. **Gate 1:** Human sign-off

### Phase 2: Stabilization (Human-Led)
- Deploy lift-and-shift models
- Run through 2+ business cycles
- Monitor and fix issues
- **Gate 2:** Stabilization sign-off

### Phase 3: ROI Analysis
- **Agent 6:** Score models by query patterns
- Tier assignment (1/2/3)
- **Gate 3:** Tier confirmation

### Phase 4: Config Optimization
- **Agent 7A:** Clustering, materialization
- **Gate 4:** Config changes sign-off

### Phase 5: SQL Optimization
- **Agent 7B:** Query rewrites
- **Gate 5:** Each Pattern C rewrite approval

---

## File Structure

```
INFA2DBT/
├── SKILL.md                    # Main orchestrator skill
├── CHANGELOG.md                # Version history
├── USAGE.md                    # Usage guide
├── DOCUMENTATION.md            # This file
├── MIGRATION.md                # Client setup instructions
├── ROLES_AND_PRIVILEGES.md     # Required permissions
├── GAPS.md                     # Parser gap analysis (34,503 XML scan)
├── agents/
│   ├── infa-xml-parser/
│   │   ├── SKILL.md
│   │   └── scripts/
│   │       └── parse_pc_xml.py          # PowerCenterParser v3.0
│   ├── infa-to-dbt-converter/
│   │   ├── SKILL.md
│   │   └── scripts/
│   │       └── agent2_batch.py          # Batch dbt model generator
│   ├── informatica-source-target/       # Agent 8 (NEW v2.2)
│   │   ├── SKILL.md
│   │   └── scripts/
│   │       └── extract_source_target.py
│   ├── conversion-fidelity-scorer/SKILL.md
│   ├── dbt-validation-critique/SKILL.md
│   ├── phase1-handoff/SKILL.md
│   ├── roi-subgraph-scorer/SKILL.md
│   └── dbt-sql-optimizer/SKILL.md
├── examples/
│   ├── example-01-simple-td-to-td/
│   ├── example-02-complex-td-to-td/
│   └── example-03-comptime-flatfile/
├── references/
│   ├── coding-guidelines-template.md
│   └── transformation-type-map.md
└── src/
    ├── agent1_parser.py           # Standalone parser (v2.x compat)
    └── agent2_converter_v2.py     # RAG-enhanced converter
```

---

## Quality Metrics

### Fidelity Score Calculation

```python
overall = (
    structure_score * 0.30 +    # CTE structure quality
    semantics_score * 0.30 +    # Logic preservation
    test_coverage * 0.20 +      # Unit test coverage
    transformation_coverage * 0.20  # % transformations converted
)
```

### Quality Thresholds

| Score Range | Status | Action |
|-------------|--------|--------|
| ≥ 0.80 | HIGH | Auto-approve |
| 0.60 - 0.79 | MEDIUM | Light review |
| < 0.60 | LOW | Full review required |

---

## Model Naming Convention (v2.1)

dbt model names use the following format to guarantee uniqueness:

```
{xml_filename}_{seq:02d}_{stg|int|mart}_{target_name}
```

| Component | Source | Example |
|-----------|--------|---------|
| `xml_filename` | Informatica XML filename (lowercased, sanitized) | `edwtd_gl_wf_ff_cap_accounts` |
| `seq` | Zero-padded target order within the XML (global per file) | `01` |
| `stg\|int\|mart` | Layer prefix based on target name heuristics | `mart_` |
| `target_name` | Target table name (lowercased) | `cap_accounts` |

**Full example:** `edwtd_gl_wf_ff_cap_accounts_01_mart_cap_accounts`

**Why XML filename?** The Informatica workflow name (embedded in the XML filename) is guaranteed unique per export. Using the mapping name (`wf_{mapping_name}`) caused collisions when different XML files contained the same mapping name.

---

## Offline / No-State Mode (v2.1)

Agent 2 can operate without a Snowflake connection:

```bash
INFA2DBT_NO_STATE=1 python3 src/agent2_converter_v2.py \
    --handoff-dir ./artifacts/handoffs \
    --output-dir ./artifacts/output \
    --no-state
```

**What is skipped in offline mode:**
- MODEL_REGISTRY insertion
- FIDELITY_SCORES recording
- PIPELINE_STATE updates
- Cortex Search corpus queries (RAG)
- Duplicate target detection warnings

**When to use:**
- `~/.snowflake/connections.toml` has TOML parsing errors
- Snowflake access is unavailable or restricted
- Local-only development or testing

---

## Error Handling

### Retry Logic
- Agent 2-3 share a retry pool (max 3 attempts)
- After 3 failures → Quarantine

### Quarantine Routing
- Missing corpus coverage → Auto-quarantine
- Low fidelity score → Quarantine
- Validation failures → Escalate

### State Recovery
- Pipeline state persisted in Snowflake
- Resume from last checkpoint on failure
- Full audit trail in tables

---

## Performance Considerations

### Cortex Search
- TARGET_LAG = '1 hour' (refresh frequency)
- Incremental refresh mode for efficiency
- Arctic embedding model for semantic search

### Warehouse Sizing
- Recommended: MEDIUM warehouse for Cortex Search builds
- Dedicated warehouse prevents workload interference

### Corpus Size
- Start with 10-20 examples per transformation type
- Add examples as edge cases are discovered
- More examples = better conversion quality
