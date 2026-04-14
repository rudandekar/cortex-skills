---
name: infa-xml-parser
description: >
  Parses Informatica PowerCenter workflow XML exports and decomposes them into
  per-target-table handoff objects for downstream dbt model generation.
  Trigger on: "Informatica workflow", "PowerCenter XML", "INFA export", "parse INFA",
  or .xml uploads with POWERMART root element.
  Do NOT use for: Talend jobs, SSIS packages, raw SQL generation.
---

# Informatica XML Parser (Agent 1) — v2.0

This skill parses Informatica PowerCenter workflow XML exports, extracts source/target
definitions, transformations, and connectors, then decomposes the workflow into N
separate handoff objects — one per distinct target table.

**Key invariant:** One Informatica workflow → N dbt models (one per target table).

**Output format:** dbt handoff JSON (NOT raw Snowflake MERGE/INSERT DML).

## What This Skill Produces

| Output | Format | Purpose |
|--------|--------|---------|
| Handoff JSON | `{model}_handoff.json` | Input for Agent 2 (dbt converter) |
| Model mapping CSV | `workflow_model_mapping.csv` | Tracking & audit |
| Execution order | Topologically sorted | dbt DAG ref() dependencies |

## Inputs

- **Required:** `workflow_xml_path` — Path to Informatica PowerCenter XML export
- **Optional:** `corpus_coverage_csv` — Path to corpus coverage CSV for gap detection
- **Optional:** `output_dir` — Output directory (default: `artifacts/`)

## Running the Parser

```bash
python scripts/parse_pc_xml.py /path/to/workflow.xml \
    --output-dir artifacts/ \
    --corpus-csv docs/corpus_coverage.csv
```

## Transformation Type Support

**Load** ../../references/transformation-type-map.md for the full transformation type support matrix.

## Workflow

### Step 1: XML Structural Validation

Validate root element is POWERMART, at least one REPOSITORY and FOLDER exist. Hard stop on failure.

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

Cross-reference transformation types against corpus. Mark each as: `ok` (3+ examples), `thin` (<3), `missing` (none), or `escalate` (unsupported type).

### Step 4: Enumerate Target Tables

Extract distinct targets with deduplication. Router output groups targeting different tables become separate target entries.

### Step 5: Trace Transformation Chains (Backwards)

For each target, walk the connector graph backwards to find all contributing transformations. Return chain in Source → Target order.

### Step 6: Build Field-Level Lineage

Trace each target field back through connectors to its source field.

### Step 7: Compute Multi-Mapping Execution Order

Topological sort on WORKFLOWLINK dependencies. This order maps to dbt `ref()` dependencies.

### Step 8: Generate dbt Model Names

| Pattern in Name | Prefix |
|----------------|--------|
| stg/staging/raw | `stg_` |
| int/intermediate | `int_` |
| All others | `mart_` |

### Step 9: Infer dbt Configuration

| Condition | Materialization |
|-----------|----------------|
| Update Strategy present | `incremental` |
| freq_hint = `incr` | `incremental` |
| freq_hint = daily/weekly/monthly | `table` |
| Default | `view` |

### Step 10: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM

Present decomposition summary: workflow name, targets found, materialization and frequency per target, transformation types, coverage gaps, dbt DAG execution order.

### Step 11: Generate Handoff Objects

JSON with keys: workflow_name, mapping_name, target_table, target_schema, proposed_model_name, model_filepath, transformation_chain (array), source_tables (array), field_lineage (array), transformation_types (array), corpus_coverage_status, freq_hint, dbt_config (materialization, schema, tags), ref_dependencies (array), pipeline_config.

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
| `_DAILY` / `_WEEKLY` / `_MONTHLY` / `_HOURLY` | corresponding |
| No match | `unknown` |
