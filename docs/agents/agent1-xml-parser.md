# Agent 1: Informatica XML Parser

**Skill Name:** `infa-xml-parser`  
**Version:** 2.3.0  
**Phase:** 1 (Conversion)  
**Script:** `src/agent1_parser.py` (435 lines)

## Purpose

Entry point for the INFA2DBT pipeline. Parses Informatica PowerCenter workflow XML exports and decomposes them into per-target-table handoff objects for downstream dbt model generation.

**Key Invariant:** One Informatica workflow → N dbt models (one per target table)

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--xml-dir` | Yes | Directory containing Informatica PowerCenter XML exports |
| `--output-dir` | No | Output directory (default: `/tmp/infa2dbt/handoffs`) |

## Outputs

| Output | Format | Purpose |
|--------|--------|---------|
| `{workflow}_{seq}_{target}_handoff.json` | JSON | Input for Agent 2 (dbt converter) |
| Execution order | Topological sort | Preserved in `execution_sequence` field |

## Workflow Steps

1. **XML Structural Validation** - Verify POWERMART root element
2. **Extract All Node Types** - Sources, Targets, Mappings, Transformations, Connectors
3. **Extract Workflow DAG** - Parse WORKFLOWLINK elements to build session dependency graph
4. **Topological Sort** - Kahn's BFS algorithm on WORKFLOWLINK DAG to determine execution order
5. **CONNECTOR-Based Target Linking** - Use `TOINSTANCETYPE="Target Definition"` to associate each target with its correct mapping (not all targets to every mapping)
6. **Trace Transformation Chains** - Walk connector graph per mapping to extract ordered transforms
7. **Build Field-Level Lineage** - Trace each target field to source via CONNECTOR elements
8. **Generate Model Names** - Apply workflow-prefixed naming: `{workflow_name}_{sequence}_{target_table_name}`
9. **Generate Handoff Objects** - Create JSON per target with `execution_sequence`, `session_name`, transformation chain, connectors, and field lineage
10. **Write Outputs** - Save handoff JSONs to output directory

## v2.3.0 Changes (from v2.0.0)

### Execution Sequence Preservation
- Parses `WORKFLOWLINK` elements (FROMTASK/TOTASK) to build a directed acyclic graph of session dependencies
- `topological_sort_sessions()` implements Kahn's BFS algorithm to determine correct execution order
- Each handoff includes `execution_sequence` (integer) and `session_name` fields

### CONNECTOR-Based Target-to-Mapping Association
- **Previous behavior (v2.0):** All targets in a FOLDER were associated with every mapping, producing inflated handoff counts (e.g., 1322 handoffs)
- **New behavior (v2.3):** Uses `CONNECTOR` elements with `TOINSTANCETYPE="Target Definition"` to link each target to its specific mapping, producing accurate counts (e.g., 937 handoffs)

### Workflow-Prefixed Naming Convention
- **Previous:** `{xml_basename}_{seq}_{model_name}` (e.g., `edwtd_gl_wf_ff_cap_accounts_01_mart_cap_accounts`)
- **New:** `{workflow_name}_{seq}_{target_table_name}` (e.g., `wf_ff_cap_accounts_01_ff_cap_accounts`)
- Workflow name directly from XML `WORKFLOW` element `NAME` attribute
- Sequence number from topological sort of workflow session DAG

## Transformation Support

| Informatica Type | dbt Pattern | Status |
|-----------------|-------------|--------|
| Source Qualifier | `source()` macro + CTE | Full |
| Expression | SELECT with calculated columns | Full |
| Filter | WHERE clause | Full |
| Aggregator | GROUP BY with aggregates | Full |
| Joiner | JOIN (INNER/LEFT/RIGHT/FULL) | Full |
| Lookup (connected) | LEFT JOIN CTE | Full |
| Lookup (unconnected) | Scalar subquery | Full |
| Router | Multiple models + WHERE | Full |
| Normalizer | LATERAL FLATTEN | Full |
| Sorter | ORDER BY | Full |
| Rank | RANK()/ROW_NUMBER() OVER | Full |
| Union | UNION ALL | Full |
| Sequence Generator | ROW_NUMBER() OVER | Full |
| Update Strategy | Incremental materialization | Full |

### Requires Escalation

- Stored Procedure (external dependency)
- Custom Transformation (opaque business logic)
- External Procedure (external dependency)

## CLI Usage

```bash
python scripts/parse_pc_xml.py /path/to/workflow.xml \
    --output-dir artifacts/ \
    --corpus-csv docs/corpus_coverage.csv
```

## Handoff JSON Schema

```json
{
  "workflow_name": "WF_FF_CAP_ACCOUNTS",
  "mapping_name": "M_FF_CAP_ACCOUNTS",
  "mapping_description": "",
  "folder_name": "EDWTD_GL",
  "target_table": "FF_CAP_ACCOUNTS",
  "target_schema": "EDW",
  "proposed_model_name": "wf_ff_cap_accounts_01_ff_cap_accounts",
  "execution_sequence": 1,
  "session_name": "S_FF_CAP_ACCOUNTS",
  "source_tables": ["CAP_ACCOUNTS"],
  "sources": [{"name": "CAP_ACCOUNTS", "schema": "raw", "fields": [...]}],
  "target": {"name": "FF_CAP_ACCOUNTS", "fields": [...]},
  "transformation_chain": [
    {"type": "Source Qualifier", "name": "SQ_CAP_ACCOUNTS", "fields": [...], "attributes": {...}}
  ],
  "transformation_types": ["Source Qualifier"],
  "connectors": [...],
  "field_lineage": [...],
  "corpus_coverage_status": "ok",
  "generated_timestamp": "2026-03-20T...",
  "agent_version": "2.3.0",
  "quarantine_flag": false
}
```

## Error Handling

| Condition | Action |
|-----------|--------|
| Malformed XML | STOP, report line/element |
| No POWERMART root | STOP, not PowerCenter XML |
| Zero targets found | STOP, structural failure |
| Unknown transform type | Log as `missing`, continue |
| Circular dependency | STOP, log cycle, escalate |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| Step 10 | MEDIUM | Decomposition summary review |
