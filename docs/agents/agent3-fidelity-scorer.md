# Agent 3: Structural Fidelity Validator

**Skill Name:** `conversion-fidelity-scorer`  
**Version:** 1.0.0  
**Phase:** 1 (Conversion)  
**Script:** `src/agent3_structural_validator.py` (602 lines)

## Purpose

Programmatically compares Informatica XML handoff metadata against generated dbt SQL models to validate structural fidelity. Answers one question: **does the generated dbt model structurally match the original Informatica mapping?**

**Key Distinction:**
- Agent 3 performs **structural/static** validation (no data execution required)
- Compares handoff JSON metadata (parsed XML) against generated SQL text
- 6-dimension weighted scoring with PASS/REVIEW/FAIL thresholds

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--handoff-dir` | Yes | Directory containing Agent 1 handoff JSONs |
| `--sql-dir` | Yes | Directory containing Agent 2 generated SQL files |
| `--report` | No | Path to write full JSON report |
| `--summary-only` | No | Print summary statistics only |

## Outputs

| Output | Format | Description |
|--------|--------|-------------|
| Console summary | Text | Pass/Review/Fail counts and component averages |
| `validation_report.json` | JSON | Full per-model validation with component scores and issues |

## Validation Dimensions

| Dimension | Weight | What It Checks |
|-----------|--------|----------------|
| Transformation Coverage | 25% | Every XML transform has a corresponding CTE (by type prefix) |
| Sequence Preservation | 15% | CTE order matches XML transform chain order (sources exempt) |
| Column Coverage | 20% | All target columns present in final SELECT |
| Expression Translation | 15% | Informatica expression fields translated (IIF→IFF, DECODE→CASE, NVL→COALESCE, SYSDATE→CURRENT_DATE) |
| Source Coverage | 10% | All source tables referenced via `source()` macro |
| Logic Validation | 15% | Filter CTEs, Lookup CTEs, Update Strategy CTEs present for corresponding XML transforms |

## Scoring

### Thresholds

| Status | Score | Action |
|--------|-------|--------|
| **PASS** | ≥ 85% | Model is structurally sound |
| **REVIEW** | 70–85% | Manual review recommended |
| **FAIL** | < 70% | Requires investigation or re-conversion |

### Scoring Rules

- **No expressions to translate** → Expression Translation scores 1.0 (nothing to translate = perfect)
- **No filters/lookups/update strategies** → Logic Validation scores 1.0 (nothing to validate = perfect)
- **Source CTEs always come first in SQL** → Sequence comparison excludes source/XML Source Qualifier types and only compares relative order of non-source transforms (sources-first is scored separately at 20% of sequence weight)
- **Final CTE column extraction** → Uses `\n\s+FROM\b` boundary to avoid matching `FROM` inside column names like `bk_from_currency_cd`

## CTE Prefix Mapping

| Informatica Type | Expected CTE Prefix |
|-----------------|---------------------|
| Source Qualifier | `source_` |
| XML Source Qualifier | `xml_parsed_` |
| Expression | `transformed_` |
| Filter | `filtered_` |
| Aggregator | `aggregated_` |
| Lookup / Lookup Procedure | `lookup_` |
| Joiner | `joined_` |
| Router | `routed_` |
| Update Strategy | `update_strategy_` |
| Normalizer | `normalized_` |
| Sequence Generator | `sequenced_` |
| Sorter | `sorted_` |

## CLI Usage

```bash
# Full validation with report
python src/agent3_structural_validator.py \
    --handoff-dir /tmp/infa2dbt/handoffs \
    --sql-dir /tmp/infa2dbt/output/models/converted \
    --report /tmp/infa2dbt/output/validation_report.json

# Summary only
python src/agent3_structural_validator.py \
    --handoff-dir /tmp/infa2dbt/handoffs \
    --sql-dir /tmp/infa2dbt/output/models/converted \
    --summary-only
```

## Validation Report Schema

```json
{
  "summary": {
    "total": 933,
    "pass": 933,
    "review": 0,
    "fail": 0,
    "skip": 0,
    "avg_score": 1.0,
    "component_averages": {
      "transformation_coverage": 1.0,
      "sequence_preservation": 1.0,
      "column_coverage": 1.0,
      "expression_translation": 1.0,
      "source_coverage": 1.0,
      "logic_validation": 1.0
    }
  },
  "models": [
    {
      "model_name": "wf_ff_cap_accounts_01_ff_cap_accounts",
      "workflow_name": "WF_FF_CAP_ACCOUNTS",
      "execution_sequence": 1,
      "overall_score": 1.0,
      "status": "PASS",
      "component_scores": {...},
      "issues": [],
      "details": {
        "transformation_coverage": {...},
        "sequence_preservation": {...},
        "column_coverage": {...},
        "expression_translation": {...},
        "filter_logic": {...},
        "lookup_logic": {...},
        "update_strategy": {...},
        "source_coverage": {...}
      }
    }
  ]
}
```

## Wave 2 Full Results (8,180 models across 6 directories)

| Directory | XMLs | Models | PASS | REVIEW | FAIL | Avg Score |
|-----------|------|--------|------|--------|------|-----------|
| Expense | ~800 | 1,035 | 1,035 | 0 | 0 | 100% |
| Common | 409 | 818 | 818 | 0 | 0 | 100% |
| IAM_Security | 188 | 239 | 239 | 0 | 0 | 100% |
| Opty_Deals_Quotes | 256 | 305 | 305 | 0 | 0 | 100% |
| Revenue_COGS | 2,112 | 3,826 | 3,819 | 7 | 0 | 99.9% |
| Bookings | 1,221 | 1,958 | 1,958 | 0 | 0 | 100% |
| **Total** | **~5,000** | **8,181** | **8,174** | **7** | **0** | **100%** |

7 REVIEW models (all in Revenue_COGS) have missing target columns in wide-column tables — these are known converter limitations requiring manual review.

## Error Handling

| Condition | Action |
|-----------|--------|
| SQL file not found | SKIP model, increment skip count |
| No handoff files | Exit with message |
| Malformed JSON | Skip model with error |
| No final CTE in SQL | Column coverage scores 0% |
