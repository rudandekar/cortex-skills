# Agent 2: Informatica to dbt Converter

**Skill Name:** `infa-to-dbt-converter`  
**Version:** 2.0.0  
**Phase:** 1 (Conversion)

## Purpose

Converts a single Informatica target table's transformation chain into a production-ready dbt model. This is the **only agent that generates SQL code** in the pipeline.

**Design Goals:**
- (a) Semantically equivalent to original Informatica mapping
- (b) Compliant with Deloitte INFA2DBT coding guidelines
- (c) Self-documenting for Agent 4 validation

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `handoff_json_path` | Yes | Path to Agent 1 handoff object |
| `coding_guidelines_path` | Yes | Path to coding guidelines document |
| `corpus_examples_dir` | No | Path to labelled corpus examples |

## Outputs

| File | Location | Description |
|------|----------|-------------|
| `{model}.sql` | `models/converted/` | dbt model SQL file |
| `{model}.schema.yml` | `models/converted/` | Column documentation and tests |
| `{model}_unit.yml` | `tests/unit/` | Unit test fixtures |
| `{model}.json` | `logs/agent2/` | Generation log |

## v2.0 Features

- **RAG Integration:** Queries `INFA2DBT_CORPUS_SEARCH` for relevant patterns
- **Persistent State:** Registers models in `MODEL_REGISTRY` table
- **Fidelity Recording:** Calculates and stores quality scores
- **Duplicate Target Detection:** Real-time warning when target already has models
- **Security Hardening:** Parameterized queries, JSON injection prevention

## Workflow Steps

1. **Parse Handoff Object** - Validate required fields
2. **Select Corpus Examples** - Few-shot examples per transformation type
3. **Generate Transformation SQL** - Apply standard conversion patterns
4. **Assemble CTE Structure** - Build complete model with CTEs in order
5. **Generate Config Block** - Apply materialization rules
6. **Generate schema.yml** - Column documentation
7. **Generate Unit Test Fixtures** - dbt native unit tests
8. **Set Unit Test Flag** - Record generation status
9. **Handle Quarantine Pre-flagging** - Insert TODO blocks for unsupported types
10. **Write Output Files** - Save all artifacts

## Transformation Patterns

### Source Qualifier
```sql
WITH source_{table} AS (
    SELECT * FROM {{ source('{schema}', '{table}') }}
)
```

### Expression
```sql
SELECT
    {original_columns},
    {expression_logic} AS {output_column}
FROM {previous_cte}
```

### Filter
```sql
SELECT * FROM {previous_cte}
WHERE {filter_condition}
```

### Aggregator
```sql
SELECT
    {group_by_columns},
    SUM({agg_column}) AS {output_sum}
FROM {previous_cte}
GROUP BY {group_by_columns}
```

### Joiner
```sql
SELECT {selected_columns}
FROM {left_cte} a
{join_type} JOIN {right_cte} b
    ON a.{join_key} = b.{join_key}
```

### Lookup (connected)
```sql
SELECT a.*, b.{lookup_return_column}
FROM {previous_cte} a
LEFT JOIN {{ source('{schema}', '{table}') }} b
    ON a.{key} = b.{key}
```

## Materialization Rules

| Model Prefix | Materialization | Override |
|-------------|-----------------|----------|
| `stg_` | view | — |
| `int_` | ephemeral | table if 3+ downstream refs |
| `mart_` | table | — |
| Any with Update Strategy | incremental | Overrides prefix rule |

## Unit Test Requirements

| Transformation | Required Test Cases |
|----------------|---------------------|
| Filter | Include pass, exclude fail, NULL handling |
| Aggregator | Multi-row group, single row, NULL in aggregate |
| Lookup | Match found, no match (NULL), multiple matches |
| Joiner | Inner exclude, left retain, cartesian guard |
| Router | Per output group validation |
| Rank | Multi-row partition, tie-breaking |

## Duplicate Target Detection

When a model targets a table that already exists in the registry:

```
[WARN] Target 'ANALYTICS.DIM_CUSTOMER' already has 2 model(s):
       - mart_customer_crm (from wf_crm_load)
       - mart_customer_legacy (from wf_legacy)
       → Consolidation opportunity detected. Review in Phase 3 (Agent 6).
```

## Error Handling

| Condition | Action |
|-----------|--------|
| Missing required fields | Hard stop, do not generate |
| SQL syntax error | Self-correct up to 2 attempts |
| Unknown transformation | TODO placeholder, quarantine flag |
| Cannot determine unique_key | TODO placeholder, quarantine flag |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| None | — | Agent 2 operates autonomously |
