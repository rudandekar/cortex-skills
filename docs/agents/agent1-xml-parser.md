# Agent 1: Informatica XML Parser

**Skill Name:** `infa-xml-parser`  
**Version:** 2.0.0  
**Phase:** 1 (Conversion)

## Purpose

Entry point for the INFA2DBT pipeline. Parses Informatica PowerCenter workflow XML exports and decomposes them into per-target-table handoff objects for downstream dbt model generation.

**Key Invariant:** One Informatica workflow → N dbt models (one per target table)

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workflow_xml_path` | Yes | Path to Informatica PowerCenter XML export |
| `corpus_coverage_csv` | No | Path to corpus coverage CSV for gap detection |
| `output_dir` | No | Output directory (default: `artifacts/`) |

## Outputs

| Output | Format | Purpose |
|--------|--------|---------|
| `{model}_handoff.json` | JSON | Input for Agent 2 (dbt converter) |
| `workflow_model_mapping.csv` | CSV | Tracking & audit |
| Execution order | Topological sort | dbt DAG ref() dependencies |

## Workflow Steps

1. **XML Structural Validation** - Verify POWERMART root element
2. **Extract All Node Types** - Sources, Targets, Transformations, Connectors
3. **Check Corpus Coverage** - Cross-reference transformation types
4. **Enumerate Target Tables** - Extract distinct targets with deduplication
5. **Trace Transformation Chains** - Walk connector graph backwards
6. **Build Field-Level Lineage** - Trace each target field to source
7. **Compute Execution Order** - Topological sort on WORKFLOWLINK dependencies
8. **Generate dbt Model Names** - Apply naming convention (stg_, int_, mart_)
9. **Infer dbt Configuration** - Materialization, schema, tags
10. **HITL Checkpoint** - Present decomposition summary for approval
11. **Generate Handoff Objects** - Create JSON for each target
12. **Write Outputs** - Save handoffs and mapping files

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
  "workflow_name": "WF_CUSTOMER_DAILY",
  "mapping_name": "M_CUSTOMER_LOAD",
  "target_table": "DIM_CUSTOMER",
  "target_schema": "EDW",
  "proposed_model_name": "mart_dim_customer",
  "transformation_chain": [...],
  "source_tables": ["raw.customer_master"],
  "field_lineage": [...],
  "transformation_types": ["Source Qualifier", "Expression"],
  "corpus_coverage_status": "ok",
  "freq_hint": "daily",
  "dbt_config": {
    "materialization": "table",
    "schema": "ANALYTICS",
    "tags": ["daily", "customer", "infa2dbt"]
  },
  "ref_dependencies": ["int_customer_staging"]
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
