# Agent 3: Conversion Fidelity Scorer

**Skill Name:** `conversion-fidelity-scorer`  
**Version:** 1.0.0  
**Phase:** 1 (Conversion)

## Purpose

Validates semantic equivalence between generated dbt models and original Informatica mapping logic. Answers one question: **does the generated dbt model produce semantically equivalent output?**

**Key Responsibility:** This is the **only agent that can route models to quarantine**.

**Critical Distinction:**
- Agent 3 scores *conversion fidelity* (correctness)
- Agent 6 scores *optimization priority* (ROI)

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_sql_path` | Yes | Path to generated dbt model SQL |
| `agent2_output_json` | Yes | Agent 2's output with quarantine_flag |
| `handoff_json_path` | Yes | Original Agent 1 handoff |
| `corpus_coverage_csv` | Yes | For threshold determination |
| `pipeline_config_yaml` | No | Fidelity thresholds (default: 0.85/0.70) |

## Outputs

| File | Location | Condition |
|------|----------|-----------|
| Pass signal | Agent 4 | All checks pass |
| Retry signal | Agent 2 | Check fails, retries remain |
| Quarantined model | `models/review_pending/` | Max retries exhausted |
| Scoring log | `logs/agent3/` | Always |

## Workflow Steps

1. **Quarantine Pre-check** - Check Agent 2's quarantine_flag
2. **Determine Threshold** - 0.85 standard, 0.70 for thin coverage
3. **Generate Synthetic Test Data** - 50-200 rows with edge cases
4. **Generate Reference Output** - Translate original logic to reference SQL
5. **Execute Comparison** - Run both models against seed data
6. **Calculate Fidelity Score** - Weighted average across 5 dimensions
7. **Execute Unit Tests** - Run dbt unit tests
8. **Generate Failure Diagnosis** - If score < threshold
9. **Routing Decision** - Pass, retry, or quarantine
10. **Write Scoring Log** - Full scoring details

## Fidelity Score Components

| Component | Weight | Pass Criteria |
|-----------|--------|---------------|
| Row count | 30% | Exact match |
| Column match | 15% | Case-insensitive name match |
| Null profile | 15% | ±2% NULL rate per column |
| Aggregate match | 25% | Exact SUM/COUNT/MAX/MIN |
| Spot check | 15% | 10 random rows match |

## Threshold Rules

| Corpus Coverage | Threshold |
|-----------------|-----------|
| All types `ok` | 0.85 |
| Any type `thin` | 0.70 |
| Any type `missing` | Quarantine (no score) |

## Routing Decision Logic

```
if fidelity_score >= threshold AND all_unit_tests_pass:
    → PASS to Agent 4
elif retry_count < 3:
    → RETRY to Agent 2 with diagnosis
else:
    → QUARANTINE to models/review_pending/
```

## Failure Diagnosis Example

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
  "likely_cause": "Filter condition too restrictive",
  "suggested_fix": "Review WHERE clause in filtered_active_vendors CTE"
}
```

## Retry Count Rules

- Shared pool: Agent 3 + Agent 4 combined ≤ 3 retries
- Example: If Agent 3 uses 2 retries, Agent 4 has 1 remaining
- On retry: targeted correction only, not full regeneration

## Error Handling

| Condition | Action |
|-----------|--------|
| Synthetic data fails | Use reduced 10-row edge case set |
| Reference SQL fails | Quarantine with `reference_sql_failure` |
| Snowflake unavailable | Hold in queue, retry later |
| Generated SQL syntax error | Score = 0.0 on all components |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| None | — | Agent 3 operates autonomously |
