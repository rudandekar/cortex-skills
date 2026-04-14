---
name: conversion-fidelity-scorer
description: >
  Validates semantic equivalence between generated DBT models and original
  Informatica mapping logic. Use this skill when validating conversion correctness,
  scoring fidelity, comparing model output against expected results, or routing
  models to pass/retry/quarantine paths. Trigger on keywords: fidelity score,
  conversion accuracy, semantic equivalence, output comparison, row count match,
  aggregate match, quarantine, retry.
  This skill is the ONLY agent that can route models to quarantine.
  Do NOT use for code style validation — use dbt-validation-critique instead.
---

# Conversion Fidelity Scorer (Agent 3)

This skill answers one question: **does the generated DBT model produce semantically
equivalent output to the original Informatica mapping?** It does not assess code
style, naming conventions, or DBT best practices — those are Agent 4's responsibility.

Agent 3 also serves as the routing decision point for the quarantine path. It is
the ONLY agent that can route a model to `models/review_pending/`.

**Critical distinction:** Agent 3 scores *conversion fidelity* (correctness).
Agent 6 scores *optimization priority* (ROI). Different questions, different phases.

## Inputs

- **Required:** `model_sql_path` — Path to generated DBT model SQL file
- **Required:** `agent2_output_json` — Agent 2's output object with quarantine_flag, transformation_types
- **Required:** `handoff_json_path` — Original Agent 1 handoff for reference
- **Required:** `corpus_coverage_csv` — For threshold determination
- **Optional:** `pipeline_config_yaml` — Fidelity thresholds (default: 0.85/0.70)

## Pre-conditions

- Agent 2 has produced a SQL file and schema.yml entry
- Handoff object contains field lineage for reference output generation
- Snowflake sandbox environment available for SQL execution

## Workflow

### Step 1: Quarantine pre-check

Before any scoring, check Agent 2's output:

```python
if agent2_output['quarantine_flag'] == True:
    # Skip all scoring — route directly to quarantine
    route_to_quarantine(
        model_path=model_sql_path,
        reason='missing_corpus_coverage',
        agent2_output=agent2_output
    )
    return  # Exit early
```

If `quarantine_flag = False`: proceed to Step 2.

### Step 2: Determine applicable threshold

```python
transform_types = agent2_output['transformation_types']
coverage_statuses = []

for t_type in transform_types:
    status = lookup_corpus_coverage(t_type, corpus_coverage_csv)
    coverage_statuses.append(status)

if 'missing' in coverage_statuses:
    # Should have been caught by quarantine pre-check
    log_anomaly('missing_coverage_reached_scoring')
    route_to_quarantine(reason='corpus_anomaly')
    return

if 'thin' in coverage_statuses:
    threshold = 0.70  # Reduced threshold for thin coverage
else:
    threshold = 0.85  # Standard threshold

log(f"Applied threshold: {threshold}")
```

### Step 3: Generate synthetic test data

For each source table in the handoff:

```python
def generate_seed_data(source_table, transformation_chain):
    # If corpus has matching example with test data, use it
    corpus_example = find_corpus_example(source_table, transformation_chain)
    if corpus_example and corpus_example.has_seed_data:
        return corpus_example.seed_data
    
    # Otherwise generate synthetic data
    seed_rows = []
    for i in range(50):  # 50-200 rows
        row = generate_row_for_schema(source_table.schema)
        seed_rows.append(row)
    
    # Add edge cases
    seed_rows.extend([
        generate_null_row(source_table.schema),
        generate_duplicate_key_row(source_table.schema),
        generate_boundary_value_row(source_table.schema)
    ])
    
    return seed_rows
```

Execute seed data as Snowflake `INSERT` statements in sandbox.

### Step 4: Generate reference output

Translate original Informatica logic to reference SQL:

```python
def generate_reference_sql(handoff):
    # Build simple SQL that reproduces expected output
    # Not optimized, not DBT conventions — just correct output
    
    reference_sql = f"""
    WITH source_data AS (
        SELECT * FROM {sandbox_seed_table}
    ),
    
    -- Apply transformations in order from field_lineage
    {build_transformation_steps(handoff['field_lineage'])}
    
    SELECT {handoff['target_columns']}
    FROM final_step
    """
    
    return reference_sql
```

### Step 5: Execute comparison

Run both the generated DBT SQL and reference SQL against seed data:

```python
# Execute generated model
generated_result = execute_sql(model_sql, sandbox_connection)

# Execute reference
reference_result = execute_sql(reference_sql, sandbox_connection)

# Compare on 5 dimensions
scores = {
    'row_count': compare_row_count(generated_result, reference_result),
    'column_match': compare_columns(generated_result, reference_result),
    'null_profile': compare_null_rates(generated_result, reference_result),
    'aggregate_match': compare_aggregates(generated_result, reference_result),
    'spot_check': compare_sample_rows(generated_result, reference_result, n=10)
}
```

### Step 6: Calculate fidelity score

```python
def calculate_fidelity_score(scores):
    weights = {
        'row_count': 0.30,
        'column_match': 0.15,
        'null_profile': 0.15,
        'aggregate_match': 0.25,
        'spot_check': 0.15
    }
    
    fidelity_score = sum(
        scores[metric] * weights[metric]
        for metric in weights
    )
    
    return fidelity_score  # 0.0 to 1.0
```

**Scoring rules per component:**
- `row_count`: 1.0 if exact match, 0.0 otherwise
- `column_match`: 1.0 if all columns match (case-insensitive), proportional for partial
- `null_profile`: 1.0 if NULL rates within ±2% per column, proportional otherwise
- `aggregate_match`: 1.0 if SUM/COUNT/MAX/MIN exact match, 0.0 if any mismatch
- `spot_check`: Proportion of 10 sampled rows that match exactly

### Step 7: Execute unit tests

```bash
dbt test --select {model_name} --store-failures type:unit
```

**Unit test result handling:**

| Result | Effect |
|--------|--------|
| All pass | No routing effect |
| Any fail | Add to retry signal (correctable error) |
| No test file + `unit_test_generated = False` | Log warning, proceed |
| `# COVERAGE GAP` comment | Log warning, proceed |

### Step 8: Generate failure diagnosis (if score < threshold)

```json
{
  "failing_components": ["row_count", "aggregate_match"],
  "row_count_expected": 142,
  "row_count_actual": 138,
  "row_count_delta": -4,
  "aggregate_failures": [
    {
      "column": "order_amount",
      "expected_sum": 142300.50,
      "actual_sum": 138200.00,
      "delta_pct": -2.88
    }
  ],
  "unit_test_failures": [
    {
      "test_name": "test_filter_excludes_inactive",
      "expected_rows": [],
      "actual_rows": [{"vendor_id": 4, "vendor_status": "INACTIVE"}]
    }
  ],
  "likely_cause": "Filter condition too restrictive — 4 rows excluded incorrectly",
  "suggested_fix": "Review WHERE clause in filtered_active_vendors CTE"
}
```

### Step 9: Routing decision

```python
retry_count = agent2_output.get('retry_count', 0)
max_retries = pipeline_config.get('max_retries', 3)

if fidelity_score >= threshold and all_unit_tests_pass:
    # PASS — proceed to Agent 4
    route_to_agent4(
        model_path=model_sql_path,
        fidelity_score=fidelity_score,
        unit_test_summary=unit_test_results
    )

elif retry_count < max_retries:
    # RETRY — return to Agent 2 with diagnosis
    route_to_agent2_retry(
        model_path=model_sql_path,
        failure_diagnosis=diagnosis,
        retry_count=retry_count + 1
    )

else:
    # QUARANTINE — max retries exhausted
    route_to_quarantine(
        model_path=model_sql_path,
        reason='fidelity_retries_exhausted',
        final_score=fidelity_score,
        failure_diagnosis=diagnosis
    )
```

### Step 10: Write scoring log

```json
{
  "model_name": "mart_dim_vendor_full",
  "workflow_name": "WF_VENDOR_FULL",
  "target_table": "DIM_VENDOR",
  "fidelity_score": 0.91,
  "threshold_applied": 0.85,
  "corpus_coverage_status": "ok",
  "retry_count": 1,
  "decision": "pass",
  "component_scores": {
    "row_count": 1.0,
    "column_match": 1.0,
    "null_profile": 0.95,
    "aggregate_match": 0.88,
    "spot_check": 0.90
  },
  "unit_test_results": {
    "status": "pass",
    "tests_run": 7,
    "tests_passed": 7,
    "tests_failed": 0
  },
  "timestamp": "2026-03-15T14:32:00Z"
}
```

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| Pass signal | Agent 4 | JSON | Model path + fidelity score + unit test summary |
| Retry signal | Agent 2 | JSON | Failure diagnosis + retry count |
| Quarantined model | `models/review_pending/` | SQL | Final attempt with TODO block |
| Scoring log | `logs/agent3/` | JSON | Full scoring details |

Inline summary must include:
- Model scored: {name}
- Fidelity score: {score} (threshold: {threshold})
- Component scores: {breakdown}
- Unit tests: {passed}/{total}
- Decision: PASS / RETRY / QUARANTINE
- Retry count: {n}/3

## Error Handling

### Synthetic data generation fails
Use reduced seed set of 10 hardcoded edge case rows. If this also fails,
quarantine with reason `seed_data_failure`.

### Reference SQL translation fails
Quarantine with reason `reference_sql_failure` — scoring not possible.

### Snowflake sandbox unavailable
Hold in pending queue. Retry when session restored. Do NOT quarantine for
infrastructure failure.

### Generated SQL produces syntax error
Score = 0.0 on all components. Include SQL error in failure diagnosis.
Route per standard retry/quarantine rules.

### Model produces correct row count but wrong columns
Score column_match = 0.0, others normally. Flag column naming in diagnosis.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| None | — | Agent 3 operates autonomously; quarantine routing is automatic |

## Reference Tables

### Fidelity Score Weights

| Component | Weight | Pass Criteria |
|-----------|--------|---------------|
| Row count | 30% | Exact match |
| Column match | 15% | Case-insensitive name match |
| Null profile | 15% | ±2% NULL rate per column |
| Aggregate match | 25% | Exact SUM/COUNT/MAX/MIN |
| Spot check | 15% | 10 random rows match |

### Threshold Rules

| Corpus Coverage | Threshold |
|-----------------|-----------|
| All types `ok` | 0.85 |
| Any type `thin` | 0.70 |
| Any type `missing` | Quarantine (no score) |
