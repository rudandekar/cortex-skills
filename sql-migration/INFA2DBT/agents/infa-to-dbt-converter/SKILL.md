---
name: infa-to-dbt-converter
version: 2.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

description: >
  Converts a single Informatica target table's transformation chain into a
  production-ready DBT model. Use this skill when you have a handoff JSON object
  from Agent 1, or when the user asks to convert, translate, or transform
  Informatica mapping logic to DBT SQL. Trigger on keywords: convert to dbt,
  informatica to snowflake, mapping conversion, generate dbt model, transformation
  chain, Source Qualifier, Expression, Aggregator, Lookup, Joiner, Filter.
  This is the ONLY agent that generates DBT SQL code.
  
  v2.0 Features:
  - RAG-enhanced conversion using Cortex Search corpus
  - Persistent state in Snowflake MODEL_REGISTRY table
  - Fidelity scoring recorded to FIDELITY_SCORES table
  
  Do NOT use for ROI scoring — use roi-subgraph-scorer instead.
  Do NOT use for validation — use dbt-validation-critique instead.

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md, INFA2DBT_coding_guidelines.md]
  snowflake_objects:
    database: INFA2DBT_DB
    schema: PIPELINE
    tables: [MODEL_REGISTRY, FIDELITY_SCORES, INFA2DBT_CORPUS]
    cortex_search: INFA2DBT_CORPUS_SEARCH
---

# Informatica to DBT Converter (Agent 2)

This skill converts a single Informatica target table's transformation chain into
a production-ready DBT model. It is the only agent in the INFA2DBT pipeline that
generates SQL code. Every design decision is oriented toward producing a model
that is:
- (a) Semantically equivalent to the original Informatica mapping
- (b) Compliant with Deloitte INFA2DBT coding guidelines
- (c) Self-documenting for Agent 4 validation

**Critical constraint:** Agent 2 operates exclusively on handoff objects from
Agent 1. It never sees the original workflow XML.

## Inputs

- **Required:** `handoff_json_path` — Path to the Agent 1 handoff object for this target
- **Required:** `coding_guidelines_path` — Path to `docs/INFA2DBT_coding_guidelines.md`
- **Optional:** `corpus_examples_dir` — Path to labelled corpus examples for few-shot

## Pre-conditions

- Valid handoff JSON with all required fields
- Coding guidelines file version matches pipeline config
- Corpus coverage status in handoff is not `missing` (if missing, quarantine flag is pre-set)

## Workflow

### Step 1: Parse handoff object and validate

```python
import json

with open(handoff_json_path) as f:
    handoff = json.load(f)

required_fields = [
    'workflow_name', 'target_table', 'target_schema',
    'proposed_model_name', 'transformation_chain', 'source_tables',
    'field_lineage', 'transformation_types', 'corpus_coverage_status'
]

for field in required_fields:
    assert field in handoff, f"Missing required field: {field}"
```

If any required field is missing: Hard stop, do not generate partial output.

### Step 2: Select corpus examples for few-shot

Based on `transformation_types` in the handoff, select relevant examples:

```python
examples_to_use = []
for t_type in handoff['transformation_types']:
    matching_examples = find_corpus_examples(t_type, corpus_examples_dir)
    examples_to_use.extend(matching_examples[:3])  # Max 3 per type
```

For `thin` coverage types: use all available examples (may be < 3).
For `missing` coverage types: skip example selection, set quarantine flag.

### Step 3: Generate transformation-specific SQL

Apply standard conversion patterns for each transformation type in the chain:

#### Source Qualifier (no SQL override)
```sql
WITH source_{table} AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
)
```

#### Source Qualifier (with SQL override)
```sql
WITH source_{table} AS (
    -- SQL Override from Informatica
    {sql_override_content}
)
```

#### Expression
```sql
SELECT
    {original_columns},
    {expression_logic} AS {output_column}
FROM {previous_cte}
```

#### Aggregator
```sql
SELECT
    {group_by_columns},
    SUM({agg_column}) AS {output_sum},
    COUNT({agg_column}) AS {output_count},
    MAX({agg_column}) AS {output_max}
FROM {previous_cte}
GROUP BY {group_by_columns}
```

#### Lookup (connected)
```sql
SELECT
    a.*,
    b.{lookup_return_column}
FROM {previous_cte} a
LEFT JOIN {{ source('{lookup_schema}', '{lookup_table}') }} b
    ON a.{lookup_key} = b.{lookup_key}
```

#### Lookup (unconnected)
```sql
SELECT
    *,
    (SELECT {return_col} FROM {lookup_table} WHERE {condition} LIMIT 1) AS {output_col}
FROM {previous_cte}
```

#### Joiner
```sql
SELECT
    {selected_columns}
FROM {left_cte} a
{join_type} JOIN {right_cte} b
    ON a.{join_key} = b.{join_key}
```

#### Filter
```sql
SELECT *
FROM {previous_cte}
WHERE {filter_condition}
```

#### Router
```sql
-- Output group: {group_name}
SELECT *
FROM {previous_cte}
WHERE {group_condition}
```

#### Normalizer (Snowflake LATERAL FLATTEN)
```sql
SELECT
    {base_columns},
    f.value AS {normalized_column}
FROM {previous_cte},
LATERAL FLATTEN(input => {array_column}) f
```

#### Rank
```sql
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {order_cols}) AS rn
FROM {previous_cte}
```

#### Update Strategy (Incremental)
```sql
{{ config(
    materialized='incremental',
    unique_key='{primary_key}',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
    {% if is_incremental() %}
    WHERE {updated_at_col} > (SELECT MAX({updated_at_col}) FROM {{ this }})
    {% endif %}
)
```

### Step 4: Assemble CTE structure

Build the complete model with CTEs in dependency order:

```sql
{{ config(
    materialized='{materialization}',
    schema='{target_schema}',
    tags=['{wf_tag}', '{freq_tag}', '{domain_tag}'],
    meta={
        'source_workflow': '{workflow_name}',
        'target_table': '{target_table}',
        'generated_by': 'INFA2DBT_accelerator_v1.1',
        'generation_timestamp': '{iso_timestamp}'
    }
) }}

-- cluster_by = ['TODO: add cluster columns based on downstream query patterns']

WITH source_{table1} AS (
    SELECT * FROM {{ source('{schema}', '{table1}') }}
),

filtered_{name} AS (
    SELECT * FROM source_{table1}
    WHERE {condition}
),

transformed_{name} AS (
    SELECT
        {columns},
        {computed_columns}
    FROM filtered_{name}
),

final AS (
    SELECT
        {output_columns}
    FROM transformed_{name}
)

SELECT * FROM final
```

### Step 5: Generate config block

Apply materialization rules:
- `stg_` prefix → `materialized='view'`
- `int_` prefix → `materialized='ephemeral'`
- `mart_` prefix → `materialized='table'`
- Update Strategy present → `materialized='incremental'`

Generate tags:
- `tag:wf_{workflow_name}` — always (lowercase, underscores)
- `tag:{freq_hint}` — from handoff, or `tag:TODO_freq` if unknown
- `tag:TODO_domain` — always (human must complete)

### Step 6: Generate schema.yml

```yaml
models:
  - name: {proposed_model_name}
    description: >
      DBT model converted from Informatica workflow {workflow_name},
      target table {target_table}. Generated by INFA2DBT accelerator v1.1.
    meta:
      source_workflow: {workflow_name}
      target_table: {target_table}
      conversion_timestamp: {iso_timestamp}
    columns:
      - name: {column_name}
        description: "Source: {source_table}.{source_column} via {transformation_type}."
        tests:
          - not_null  # If primary key or NOT NULL constraint
          - unique    # If primary key
```

### Step 7: Generate unit test fixtures

Create DBT native unit test YAML for each transformation type:

```yaml
unit_tests:
  - name: test_{model}_filter_includes_active
    model: {proposed_model_name}
    given:
      - input: source('{schema}', '{table}')
        rows:
          - {col1: val1, col2: val2, status: 'ACTIVE'}
          - {col1: val3, col2: val4, status: 'INACTIVE'}
    expect:
      rows:
        - {col1: val1, col2: val2, status: 'ACTIVE'}

  - name: test_{model}_aggregator_sums_correctly
    model: {proposed_model_name}
    given:
      - input: ref('{upstream_model}')
        rows:
          - {key: 1, amount: 100}
          - {key: 1, amount: 200}
    expect:
      rows:
        - {key: 1, total_amount: 300}
```

**Standard test cases per transformation type:**

| Type | Test Cases Required |
|------|---------------------|
| Filter | Include pass, exclude fail, NULL handling |
| Aggregator | Multi-row group, single row, NULL in aggregate |
| Lookup (connected) | Match found, no match (NULL), multiple matches |
| Lookup (unconnected) | Scalar hit, scalar miss |
| Joiner | Inner exclude, left retain, cartesian guard |
| Router | Per output group: match this, exclude others |
| Rank | Multi-row partition, tie-breaking |
| Update Strategy | is_incremental false, true, boundary |
| Normalizer | N values → N rows, NULL handling |
| Expression | Standard input, NULL input |

### Step 8: Set unit test generation flag

In the generation log, record:
```json
{
  "unit_test_generated": true,
  "unit_test_count": 5,
  "unit_test_file": "tests/unit/{model}_unit.yml"
}
```

If unit tests could not be generated (rare transformations with no test pattern):
```json
{
  "unit_test_generated": false,
  "unit_test_skip_reason": "No test pattern defined for transformation type: {type}"
}
```

### Step 9: Handle quarantine pre-flagging

If `corpus_coverage_status = missing` for any transformation type:

1. Generate best-effort SQL for handleable portions
2. Insert TODO block at unsupported transformation point:

```sql
-- ============================================================
-- TODO: REVIEW REQUIRED — Unsupported transformation type
-- Transformation type: {type}
-- Reason: No labelled examples in corpus (coverage_status = missing)
-- Informatica transformation node: {node_name}
-- Suggested approach: {best_effort_description}
-- Review owner: Data Engineering Lead
-- Reprocess: Submit corrected SQL to Agent 4 (skip Agents 1-3)
-- ============================================================
```

3. Set `quarantine_flag = True` in output object

### Step 9: Write output files

Write to designated locations:
- Model SQL: `models/converted/{proposed_model_name}.sql`
- Schema YAML: `models/converted/{proposed_model_name}.schema.yml`
- Unit tests: `tests/unit/{proposed_model_name}_unit.yml`
- Generation log: `logs/agent2/{proposed_model_name}.json`

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| `{model}.sql` | `models/converted/` | SQL | DBT model file |
| `{model}.schema.yml` | `models/converted/` | YAML | Column documentation and tests |
| `{model}_unit.yml` | `tests/unit/` | YAML | Unit test fixtures |
| `{model}.json` | `logs/agent2/` | JSON | Generation log |

Inline summary must include:
- Model name generated
- Transformation types converted: [list]
- Lines of SQL generated
- Unit tests generated: N
- Quarantine flag: true/false
- Corpus gaps: [list if any]

## Error Handling

### Handoff object missing required fields
Hard stop. Log missing fields. Do not generate partial output.

### SQL generation produces syntax error
Self-correct up to 2 internal attempts. Pass to Agent 3 with `internal_retry_count` noted.

### Transformation type is unknown
Generate TODO placeholder. Set `quarantine_flag = True`. Continue with rest of model.

### unique_key cannot be determined for incremental
Use `TODO: define unique_key` placeholder. Log warning. Set `quarantine_flag = True`.

### Output column count mismatch
Log as warning (source definition may be stale). Include in Agent 3 handoff.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| None | — | Agent 2 operates autonomously; quality gates are in Agent 3 and 4 |

## Reference Tables

### Materialization Rules

| Model Prefix | Materialization | Override Condition |
|-------------|-----------------|-------------------|
| `stg_` | view | — |
| `int_` | ephemeral | table if 3+ downstream refs |
| `mart_` | table | — |
| Any with Update Strategy | incremental | Overrides prefix rule |

### Tag Generation Rules

| Tag Type | Value | Source |
|----------|-------|--------|
| Workflow | `wf_{workflow_name}` | Handoff, always |
| Frequency | `{freq_hint}` or `TODO_freq` | Handoff or stub |
| Domain | `TODO_domain` | Always stub |
