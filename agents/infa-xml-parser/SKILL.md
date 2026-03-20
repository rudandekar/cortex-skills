---
name: infa-xml-parser
version: 2.2.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-19
status: active

description: >
  Parses Informatica PowerCenter workflow XML exports and decomposes them into
  per-target-table handoff objects for downstream dbt model generation.
  
  TRIGGER CONDITIONS (require "Informatica" OR "PowerCenter" alongside):
  - User uploads .xml with POWERMART root element
  - Keywords: "Informatica workflow", "PowerCenter XML", "INFA export", "parse INFA"
  - XML elements: SOURCE, TARGET, TRANSFORMATION, CONNECTOR, SESSION, WORKFLOW, MAPPING
  
  DO NOT trigger on generic mentions of "mappings" or "transformations" without
  Informatica/PowerCenter context.
  
  OUTPUT: dbt handoff JSON objects (NOT raw Snowflake DML). Handoffs include:
  - dbt model name with prefix (stg_, int_, mart_)
  - Materialization type (table, incremental, view)
  - Schema strategy
  - Tags (frequency, domain)
  - ref() dependencies for DAG ordering
  
  Do NOT use for: Talend jobs, SSIS packages, raw SQL generation.

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md]
  scripts:
    - scripts/parse_pc_xml.py
---

# Informatica XML Parser (Agent 1) — v2.2

This skill is the entry point for the INFA2DBT accelerator pipeline. It parses
Informatica PowerCenter workflow XML exports, extracts all source definitions,
target definitions, transformations, and connectors, then decomposes the workflow
into N separate handoff objects — one per distinct target table.

**Key invariant:** One Informatica workflow → N dbt models (one per target table).

**Output format:** dbt handoff JSON (NOT raw Snowflake MERGE/INSERT DML).

## What This Skill Produces

| Output | Format | Purpose |
|--------|--------|---------|
| Handoff JSON | `{model}_handoff.json` | Input for Agent 2 (dbt converter) |
| Model mapping CSV | `workflow_model_mapping.csv` | Tracking & audit |
| Execution order | Topologically sorted | dbt DAG ref() dependencies |

The handoff JSON includes dbt-specific configuration:
- `materialization`: table, incremental, view
- `schema`: target schema for dbt
- `tags`: [frequency, domain, 'infa2dbt']
- `ref_dependencies`: upstream model names for DAG

## Inputs

- **Required:** `workflow_xml_path` — Path to Informatica PowerCenter XML export
- **Optional:** `corpus_coverage_csv` — Path to corpus coverage CSV for gap detection
- **Optional:** `output_dir` — Output directory (default: `artifacts/`)

## Running the Parser

### Option 1: Python Script (Recommended)

```bash
python scripts/parse_pc_xml.py /path/to/workflow.xml \
    --output-dir artifacts/ \
    --corpus-csv docs/corpus_coverage.csv
```

### Option 2: Inline Parsing

If the script is unavailable, use the inline Python in this skill file.

## Transformation Type Support

### Fully Supported (corpus coverage required)

| Informatica Type | dbt Pattern | Notes |
|-----------------|-------------|-------|
| Source Qualifier | `source()` macro + CTE | SQL Override → raw SQL in CTE |
| Expression | SELECT with calculated columns | Port expressions → SQL |
| Filter | WHERE clause | FILTERCONDITION → predicate |
| Aggregator | GROUP BY with aggregates | Group ports → GROUP BY cols |
| Joiner | JOIN (INNER/LEFT/RIGHT/FULL) | Join type → SQL join |
| Lookup (connected) | LEFT JOIN CTE | Lookup SQL → CTE definition |
| Lookup (unconnected) | Scalar subquery | Inline lookup |
| Router | Multiple models + WHERE | Each group → separate model |
| Normalizer | LATERAL FLATTEN | Array/struct unpivot |
| Sorter | ORDER BY | Sort keys (usually implicit) |
| Rank | RANK()/ROW_NUMBER() OVER | Window function |
| Union | UNION ALL | Combine CTEs |
| Sequence Generator | ROW_NUMBER() OVER | Surrogate key generation |
| Update Strategy | Incremental materialization | DD_INSERT/DD_UPDATE logic |

### Requires Escalation

| Informatica Type | Reason |
|-----------------|--------|
| Stored Procedure | External dependency |
| Custom Transformation | Business logic opaque |
| External Procedure | External dependency |

## Workflow

### Step 1: XML Structural Validation

```python
import xml.etree.ElementTree as ET

tree = ET.parse(workflow_xml_path)
root = tree.getroot()
assert root.tag == 'POWERMART', f"Expected POWERMART root, got {root.tag}"
```

Validate:
- Root element is `POWERMART`
- At least one `REPOSITORY` child exists
- At least one `FOLDER` element exists

**On failure:** STOP, report specific error, do not proceed.

### Step 2: Extract All Node Types

| Node Type | XML Element | Key Attributes |
|-----------|-------------|----------------|
| Sources | `SOURCE` | NAME, DBDNAME, DATABASETYPE, OWNERNAME |
| Targets | `TARGET` | NAME, DBDNAME, TABLENAME, DATABASETYPE |
| Transformations | `TRANSFORMATION` | TYPE, NAME, TRANSFORMFIELD[], TABLEATTRIBUTE[] |
| Connectors | `CONNECTOR` | FROMINSTANCE, TOINSTANCE, FROMFIELD, TOFIELD |
| Sessions | `SESSION` | NAME, MAPPINGNAME, CONNECTIONREFERENCE[] |
| Workflows | `WORKFLOW` | NAME, TASK[], WORKFLOWLINK[] |
| Mappings | `MAPPING` | NAME, ISVALID |
| Mapplets | `MAPPLET` | NAME (for reusable transformations) |

### Step 3: Check Corpus Coverage

Cross-reference transformation types against corpus:

```python
coverage_status = {}
for t_type in transform_types_found:
    if t_type in ESCALATE_TYPES:
        coverage_status[t_type] = 'escalate'
    elif t_type not in corpus:
        coverage_status[t_type] = 'missing'
    elif corpus[t_type]['example_count'] < 3:
        coverage_status[t_type] = 'thin'
    else:
        coverage_status[t_type] = 'ok'
```

### Step 4: Enumerate Target Tables

Extract distinct targets with deduplication:

```python
targets = []
for target in root.iter('TARGET'):
    targets.append({
        'target_table_name': target.get('TABLENAME', target.get('NAME')),
        'target_schema_name': target.get('DBDNAME') or target.get('OWNERNAME'),
        'fields': [f.attrib for f in target.findall('.//TARGETFIELD')]
    })
```

**Router handling:** Each Router output group targeting different tables → separate target entry.

### Step 5: Trace Transformation Chains (Backwards)

For each target, walk the connector graph backwards to find all contributing transformations:

```python
def trace_chain(target_name, connectors, transformations):
    chain = []
    visited = set()
    queue = [target_name]
    
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        
        incoming = [c for c in connectors if c['toinstance'] == current]
        for conn in incoming:
            from_instance = conn['frominstance']
            queue.append(from_instance)
            transform = get_transform(from_instance, transformations)
            if transform:
                chain.append(transform)
    
    return list(reversed(chain))  # Source → Target order
```

### Step 6: Build Field-Level Lineage

Trace each target field back to its source:

```python
field_lineage = []
for target_field in target['fields']:
    path = trace_field_lineage(target_field['NAME'], target_name, connectors)
    field_lineage.append({
        'target_field': target_field['NAME'],
        'datatype': target_field['DATATYPE'],
        'source_path': path
    })
```

### Step 7: Compute Multi-Mapping Execution Order

Use topological sort on WORKFLOWLINK dependencies:

```python
def compute_execution_order(workflows):
    in_degree = defaultdict(int)
    for workflow in workflows:
        for link in workflow['links']:
            in_degree[link['totask']] += 1
    
    queue = [n for n in all_nodes if in_degree[n] == 0]
    order = []
    
    while queue:
        node = queue.pop(0)
        order.append(node)
        for link in find_links_from(node):
            in_degree[link['totask']] -= 1
            if in_degree[link['totask']] == 0:
                queue.append(link['totask'])
    
    return order
```

This order maps to dbt `ref()` dependencies.

### Step 8: Generate dbt Model Names

Apply naming convention with XML filename prefix, sequence number, and layer prefix:

```python
xml_basename = Path(xml_path).stem.lower().replace(' ', '_').replace('-', '_')
global_seq = [0]  # Per-XML sequence counter

def propose_model_name(target_table: str, xml_basename: str, seq: int) -> str:
    clean = target_table.lower().replace(' ', '_')
    
    if any(x in clean for x in ('stg', 'staging', 'raw')):
        base = f"stg_{clean}" if not clean.startswith('stg') else clean
    elif any(x in clean for x in ('int', 'intermediate')):
        base = f"int_{clean}" if not clean.startswith('int') else clean
    else:
        base = f"mart_{clean}" if not clean.startswith('mart') else clean
    
    return f"{xml_basename}_{seq:02d}_{base}"
```

**Format:** `{xml_filename}_{seq:02d}_{stg|int|mart}_{target_name}`

**Example:**
- XML: `EDWTD_GL_wf_FF_CAP_ACCOUNTS.xml`, target #1: `CAP_ACCOUNTS`
- Model: `edwtd_gl_wf_ff_cap_accounts_01_mart_cap_accounts`

The XML filename prefix guarantees uniqueness across workflows. The sequence
number preserves target execution order within each workflow.

### Step 9: Infer dbt Configuration

```python
dbt_config = {
    'materialization': infer_materialization(target, freq_hint),
    'schema': infer_schema(target),
    'tags': infer_tags(workflow_name, freq_hint),
    'pre_hook': [],
    'post_hook': []
}

def infer_materialization(target, freq_hint):
    if 'Update Strategy' in target['transform_types']:
        return 'incremental'
    if freq_hint == 'incr':
        return 'incremental'
    if freq_hint in ('daily', 'weekly', 'monthly'):
        return 'table'
    return 'view'
```

### Step 10: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM

Present decomposition summary for approval:

```
Workflow: WF_CUSTOMER_DAILY
Targets found: 3
  1. mart_dim_customer (table, daily)
  2. mart_fact_orders (incremental, daily)
  3. int_customer_staging (view)

Transform types: [Source Qualifier, Expression, Aggregator, Joiner, Update Strategy]
Coverage gaps: [Aggregator: thin]

Execution order (dbt DAG):
  1. int_customer_staging
  2. mart_dim_customer
  3. mart_fact_orders (refs: mart_dim_customer)

Proceed with handoff generation? [Y/N]
```

**On rejection:** Ask which targets to exclude or modify.

### Step 11: Generate Handoff Objects

```json
{
  "workflow_name": "WF_CUSTOMER_DAILY",
  "mapping_name": "M_CUSTOMER_LOAD",
  "target_table": "DIM_CUSTOMER",
  "target_schema": "EDW",
  "proposed_model_name": "wf_customer_daily_01_mart_dim_customer",
  "model_filepath": "models/converted/wf_customer_daily_01_mart_dim_customer.sql",
  "transformation_chain": [...],
  "source_tables": ["raw.customer_master", "raw.customer_address"],
  "field_lineage": [...],
  "transformation_types": ["Source Qualifier", "Expression", "Aggregator"],
  "corpus_coverage_status": "ok",
  "freq_hint": "daily",
  "dbt_config": {
    "materialization": "table",
    "schema": "ANALYTICS",
    "tags": ["daily", "customer", "infa2dbt"],
    "pre_hook": [],
    "post_hook": []
  },
  "ref_dependencies": ["int_customer_staging"],
  "pipeline_config": {
    "fidelity_threshold": 0.85,
    "max_retries": 3,
    "quarantine_on_missing": false
  },
  "agent1_timestamp": "2026-03-15T12:00:00Z",
  "agent1_version": "2.0.0"
}
```

### Step 12: Write Outputs

| File | Location | Purpose |
|------|----------|---------|
| `{model}_handoff.json` | `artifacts/handoffs/` | Agent 2 input |
| `workflow_model_mapping.csv` | `artifacts/` | Tracking |
| `agent1_run.json` | `artifacts/logs/` | Audit log |

## Error Handling

| Condition | Action |
|-----------|--------|
| Malformed XML | STOP, report line/element |
| No POWERMART root | STOP, not PowerCenter XML |
| Zero targets found | STOP, structural failure |
| Unknown transform type | Log as `missing`, continue, flag for review |
| Router ambiguity | HITL checkpoint for clarification |
| Circular dependency | STOP, log cycle, escalate |
| Mapplet reference | Expand inline, trace through |

## Frequency Hint Inference

| Pattern in Name | freq_hint |
|----------------|-----------|
| `_INCR` | `incr` |
| `_FULL` | `full` |
| `_DAILY` | `daily` |
| `_WEEKLY` | `weekly` |
| `_MONTHLY` | `monthly` |
| `_HOURLY` | `hourly` |
| No match | `unknown` |

## Output Summary Template

```
═══════════════════════════════════════════════════════════════
AGENT 1 PARSING COMPLETE
═══════════════════════════════════════════════════════════════
Workflows processed: 1
Mappings found: 2
Target tables extracted: 3
Source tables identified: 5
Transformation types: [Source Qualifier, Expression, Aggregator, Joiner, Update Strategy]

Corpus coverage:
  ✓ Source Qualifier: ok (12 examples)
  ✓ Expression: ok (15 examples)
  ⚠ Aggregator: thin (2 examples)
  ✓ Joiner: ok (8 examples)
  ✓ Update Strategy: ok (5 examples)

dbt models ready for Agent 2: 3
  1. mart_dim_customer (table) → refs: []
  2. mart_fact_orders (incremental) → refs: [mart_dim_customer]
  3. int_customer_staging (view) → refs: []

Handoffs written to: artifacts/handoffs/
═══════════════════════════════════════════════════════════════
```
