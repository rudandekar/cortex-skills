---
name: infa-to-dbt-converter
description: >
  Converts a single Informatica target table's transformation chain into a
  production-ready DBT model. Use this skill when you have a handoff JSON object
  from Agent 1, or when the user asks to convert, translate, or transform
  Informatica mapping logic to DBT SQL. Trigger on keywords: convert to dbt,
  informatica to snowflake, mapping conversion, generate dbt model, transformation
  chain, Source Qualifier, Expression, Aggregator, Lookup, Joiner, Filter.
  This is the ONLY agent that generates DBT SQL code.
  Do NOT use for ROI scoring — use roi-subgraph-scorer instead.
  Do NOT use for validation — use dbt-validation-critique instead.
---

# Informatica to DBT Converter (Agent 2)

This skill converts a single Informatica target table's transformation chain into
a production-ready DBT model. It is the only agent in the INFA2DBT pipeline that
generates SQL code. Every design decision is oriented toward producing a model
that is: (a) semantically equivalent, (b) compliant with coding guidelines, and
(c) self-documenting for Agent 4 validation.

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

Validate all required fields: workflow_name, target_table, target_schema, proposed_model_name, transformation_chain, source_tables, field_lineage, transformation_types, corpus_coverage_status. Hard stop on missing fields.

### Step 2: Select corpus examples for few-shot

For each transformation_type in the handoff, select up to 3 matching corpus examples. For `thin` coverage: use all available. For `missing`: skip, set quarantine flag.

### Step 3: Generate transformation-specific SQL

For each transformation type in the chain, generate the corresponding SQL pattern.

**Load** references/transformation-patterns.md for SQL templates per transformation type.

### Step 4: Assemble CTE structure

Build the complete model with CTEs in dependency order: source CTEs first (using `{{ source() }}`), then transformation CTEs in chain order, ending with a `final` CTE. Include `{{ config() }}` block at the top with materialization, schema, tags, and meta.

### Step 5: Generate config block

Apply materialization rules: `stg_` → view, `int_` → ephemeral, `mart_` → table, Update Strategy → incremental. Generate tags: `wf_{workflow_name}`, `{freq_hint}` or `TODO_freq`, `TODO_domain`.

### Step 6: Generate schema.yml

Create schema.yml with model description (referencing source workflow), column descriptions (tracing source lineage through transformations), and tests (not_null/unique for PKs).

### Step 7: Generate unit test fixtures

Create DBT native unit test YAML with test cases per transformation type:

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

Record in generation log: unit_test_generated (bool), unit_test_count, unit_test_file path.

### Step 9: Handle quarantine pre-flagging

If any transformation type has `corpus_coverage_status = missing`: generate best-effort SQL, insert TODO block at unsupported transformation point, set `quarantine_flag = True`.

### Step 10: Write output files

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

## Error Handling

| Condition | Action |
|-----------|--------|
| Missing handoff fields | Hard stop, log missing fields |
| SQL syntax error | Self-correct up to 2 attempts, pass to Agent 3 |
| Unknown transformation type | TODO placeholder, quarantine_flag = True |
| unique_key undetermined | TODO placeholder, quarantine_flag = True |
| Column count mismatch | Warning, include in Agent 3 handoff |

## HITL Checkpoints Summary

Agent 2 operates autonomously; quality gates are in Agent 3 and 4.

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
