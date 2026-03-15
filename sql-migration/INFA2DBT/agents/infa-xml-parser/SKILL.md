---
name: infa-xml-parser
version: 1.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

description: >
  Parses Informatica PowerCenter workflow XML exports and decomposes them into
  per-target-table handoff objects for downstream conversion. Use this skill when
  the user uploads a .xml file containing POWERMART elements, mentions "Informatica
  workflow", "PowerCenter XML", "INFA export", or asks to parse, extract, or analyze
  Informatica mappings. Trigger on keywords: SOURCE, TARGET, TRANSFORMATION,
  CONNECTOR, SESSION, WORKFLOW, MAPPING, Source Qualifier, Expression, Aggregator,
  Lookup, Joiner, Filter, Router, Normalizer, Rank, Update Strategy.
  Do NOT use for Talend jobs — use talend-job-parser instead.
  Do NOT use for SSIS packages — use ssis-package-parser instead.

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md]
---

# Informatica XML Parser (Agent 1)

This skill is the entry point for the INFA2DBT accelerator pipeline. It parses
Informatica PowerCenter workflow XML exports, extracts all source definitions,
target definitions, transformations, and connectors, then decomposes the workflow
into N separate handoff objects — one per distinct target table. Each handoff
object contains the complete transformation chain required to populate that target.

**Key invariant:** One Informatica workflow → N DBT models (one per target table).

## Inputs

- **Required:** `workflow_xml_path` — Path to the Informatica PowerCenter XML export file
- **Required:** `corpus_coverage_csv` — Path to `docs/corpus_coverage.csv` for transformation type coverage lookup
- **Optional:** `scheduling_audit_xlsx` — Path to `docs/scheduling_audit.xlsx` for frequency hint resolution
- **Optional:** `output_dir` — Directory for output artifacts (default: `artifacts/`)

## Pre-conditions

- XML file must be valid PowerCenter export format (POWERMART root element)
- `corpus_coverage.csv` must exist with columns: `transform_type`, `example_count`, `status`
- All pre-flight items must be complete (corpus audit, scheduling audit, coding guidelines)

## Workflow

### Step 1: XML structural validation

Read the XML file and validate the root structure:
- Assert root element is `POWERMART` with `CREATION_DATE` and `REPOSITORY_VERSION` attributes
- Assert at least one `REPOSITORY` child element exists
- Assert at least one `FOLDER` element exists within the repository
- If validation fails: STOP, report specific structural error, do not proceed

```python
import xml.etree.ElementTree as ET

tree = ET.parse(workflow_xml_path)
root = tree.getroot()
assert root.tag == 'POWERMART', f"Expected POWERMART root, got {root.tag}"
```

### Step 2: Extract all node types

Parse and extract all required node types into structured dictionaries:

| Node Type | XML Element | Fields to Capture |
|-----------|-------------|-------------------|
| Sources | `SOURCE` | name, dbdname, databasetype, ownername, all SOURCEFIELD children |
| Targets | `TARGET` | name, dbdname, databasetype, ownername, all TARGETFIELD children |
| Transformations | `TRANSFORMATION` | type, name, all TRANSFORMFIELD and TABLEATTRIBUTE children |
| Connectors | `CONNECTOR` | frominstance, fromfield, toinstance, tofield |
| Sessions | `SESSION` | name, mappingname, all CONNECTIONREFERENCE and SESSIONEXTENSION |
| Workflows | `WORKFLOW` | name, all TASK and WORKFLOWLINK children |
| Mappings | `MAPPING` | name, isvalid, all contained transformations |

Log counts for each node type extracted.

### Step 3: Identify transformation types and check corpus coverage

For each `TRANSFORMATION` element, extract the `TYPE` attribute and cross-reference
against `corpus_coverage.csv`:

```python
transform_types_found = set()
for transform in all_transformations:
    t_type = transform.get('TYPE')
    transform_types_found.add(t_type)

coverage_status = {}
for t_type in transform_types_found:
    row = corpus_df[corpus_df['transform_type'] == t_type]
    if row.empty:
        coverage_status[t_type] = 'missing'
    elif row['example_count'].iloc[0] < 3:
        coverage_status[t_type] = 'thin'
    else:
        coverage_status[t_type] = 'ok'
```

Log any `missing` or `thin` coverage status — these will trigger quarantine or
reduced threshold in Agent 3.

### Step 4: Enumerate target tables (decomposition)

Extract the distinct list of target tables from `TARGET` elements:

```python
targets = []
for target in root.iter('TARGET'):
    targets.append({
        'target_instance_name': target.get('NAME'),
        'target_table_name': target.get('NAME'),  # or TABLENAME if different
        'target_schema_name': target.get('DBDNAME') or target.get('OWNERNAME'),
        'database_type': target.get('DATABASETYPE'),
        'fields': [f.attrib for f in target.findall('.//TARGETFIELD')]
    })
```

**Rules:**
- Deduplicate by physical table name if multiple instances write to same table
- For Router transformations, each output group routing to different targets = separate target

### Step 5: Build per-target transformation chains

For each target identified in Step 4, trace the connector graph backwards to
identify all transformations that contribute to it:

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
        
        # Find all connectors TO this instance
        incoming = [c for c in connectors if c['toinstance'] == current]
        for conn in incoming:
            from_instance = conn['frominstance']
            queue.append(from_instance)
            
            # Get transformation details
            transform = next((t for t in transformations 
                            if t['name'] == from_instance), None)
            if transform:
                chain.append(transform)
    
    return chain
```

### Step 6: Extract field-level lineage

For each target, build the complete field lineage map:

```python
field_lineage = []
for target_field in target['fields']:
    lineage_path = trace_field_lineage(
        target_field['NAME'],
        target['target_instance_name'],
        connectors
    )
    field_lineage.append({
        'target_field': target_field['NAME'],
        'source_path': lineage_path,
        'transformations_applied': [t['type'] for t in lineage_path]
    })
```

### Step 7: Generate proposed model names

Apply naming convention from coding guidelines:

```python
def propose_model_name(target_table, workflow_name, prefix='mart_'):
    # Clean the table name
    clean_name = target_table.lower().replace(' ', '_')
    
    # Determine prefix based on target type
    if 'stg' in clean_name or 'staging' in clean_name:
        prefix = 'stg_'
    elif 'int' in clean_name or 'intermediate' in clean_name:
        prefix = 'int_'
    else:
        prefix = 'mart_'
    
    return f"{prefix}{clean_name}"
```

### Step 8: Extract scheduling metadata stub

Extract scheduling hints from the XML that Agent 1 CAN determine:

```python
scheduling_stub = {
    'workflow_name': workflow.get('NAME'),
    'session_task_names': [s.get('NAME') for s in sessions],
    'session_task_order': extract_task_order(workflow_links),
    'freq_hint': infer_freq_from_name(workflow.get('NAME')),  # _INCR, _FULL, _DAILY
    'mapping_names': [m.get('NAME') for m in mappings],
    # Human-completed fields (leave empty)
    'schedule_frequency': None,
    'schedule_tag': None,
    'domain_tag': None,
    'inter_schedule_dependencies': None,
    'upstream_feed_sensors': None,
    'sla_window': None,
    'retry_rules': None
}
```

### Step 9: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Confirm decomposition

Present to the user:
- Total workflows found: N
- Total target tables identified: M
- Target table list with proposed model names
- Any transformation types with `thin` or `missing` corpus coverage
- Any Router transformations requiring output group confirmation

**Required response:** Explicit approval to proceed with decomposition.
On rejection: Ask which targets to exclude or how to handle Router ambiguity.

### Step 10: Write mapping table records

Append one row per target table to `workflow_model_mapping.csv`:

| Column | Value |
|--------|-------|
| workflow_name | From WORKFLOW.name |
| target_table | From TARGET.tablename |
| target_schema | From TARGET.dbdname |
| proposed_model_name | From Step 7 |
| model_filepath | `models/converted/{proposed_model_name}.sql` |
| transformation_types | Comma-separated list from chain |
| source_tables | Comma-separated list |
| corpus_coverage_status | From Step 3 lookup |
| agent1_timestamp | Current ISO8601 |
| agent1_status | `complete` or `escalated` |

### Step 11: Write scheduling stub

Append one row per workflow to `scheduling_stub.csv` with fields from Step 8.

### Step 12: Generate handoff objects

For each target, produce a JSON handoff object:

```json
{
  "workflow_name": "WF_VENDOR_FULL",
  "target_table": "DIM_VENDOR",
  "target_schema": "EDW",
  "proposed_model_name": "mart_dim_vendor_full",
  "model_filepath": "models/converted/mart_dim_vendor_full.sql",
  "transformation_chain": [...],
  "source_tables": ["raw.vendor_master"],
  "field_lineage": [...],
  "transformation_types": ["Source Qualifier", "Expression", "Aggregator"],
  "corpus_coverage_status": "ok",
  "freq_hint": "full",
  "pipeline_config": {
    "fidelity_threshold": 0.85,
    "max_retries": 3
  }
}
```

Save to `artifacts/handoffs/{proposed_model_name}_handoff.json`.

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| `workflow_model_mapping.csv` | `artifacts/` | CSV | One row per target table |
| `scheduling_stub.csv` | `artifacts/` | CSV | One row per workflow |
| `{model}_handoff.json` | `artifacts/handoffs/` | JSON | Per-target handoff for Agent 2 |
| `agent1_run.json` | `logs/agent1/` | JSON | Full extraction log |

Inline summary must include:
- Workflows processed: N
- Target tables extracted: M
- Transformation types found: [list]
- Corpus coverage gaps: [list of thin/missing]
- Models ready for Agent 2: M

## Error Handling

### Malformed XML
Hard stop. Report specific element and line number. Do not attempt partial processing.

### Unknown transformation type
Log as `unknown_transform_type`. Set `corpus_coverage_status = missing`. Continue
processing other targets. Flag for human review.

### Router with ambiguous output groups
Log as `router_ambiguity`. Produce best-effort decomposition. Trigger HITL checkpoint
for clarification before writing handoffs.

### Zero target tables found
Hard stop. This indicates structural parsing failure. Escalate to human.

### Circular dependency in connector graph
Hard stop. Log cycle nodes. Escalate — indicates data model issue in source.

### Target table name conflict
Log warning. Add `_v2` suffix to proposed model name. Flag for human review.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Step 9 | 🟡 MEDIUM | Decomposition confirmation before writing artifacts |

## Reference Tables

### Frequency Hint Inference

| Workflow Name Pattern | Inferred freq_hint |
|----------------------|-------------------|
| Contains `_INCR` | `incr` |
| Contains `_FULL` | `full` |
| Contains `_DAILY` | `daily` |
| Contains `_WEEKLY` | `weekly` |
| Contains `_MONTHLY` | `monthly` |
| No pattern match | `unknown` |

### Transformation Type Recognition

| Informatica Type | Status |
|-----------------|--------|
| Source Qualifier | Standard |
| Expression | Standard |
| Lookup | Standard |
| Aggregator | Standard |
| Router | Standard |
| Joiner | Standard |
| Filter | Standard |
| Normalizer | Rare |
| Rank | Standard |
| Update Strategy | Standard |
| Sequence Generator | Standard |
| Stored Procedure | Escalate |
| Custom | Escalate |
