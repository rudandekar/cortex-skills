# Agent 6: ROI Subgraph Scorer

**Skill Name:** `roi-subgraph-scorer`  
**Version:** 1.0.0  
**Phase:** 3 (Optimization Analysis)

## Purpose

Answers the question: **which models should we optimize first?** Analyzes DAG subgraphs to identify high-ROI optimization targets based on query frequency, scan volume, credit consumption, and centrality.

**Critical Timing:** Runs after Phase 2 stabilization (2+ business cycles of production data).

## Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dbt_project_path` | Yes | Path to dbt project root |
| `snowflake_connection` | Yes | Connection with INFORMATION_SCHEMA access |
| `analysis_window_days` | Yes | Days of query history (default: 30) |
| `weight_config` | No | Custom ROI weights |

## Outputs

| File | Location | Description |
|------|----------|-------------|
| ROI analysis | `docs/phase3/roi_analysis.json` | Full analysis JSON |
| Tier 1 report | `docs/phase3/tier1_models.md` | Human-readable |
| Optimization plan | `docs/phase3/optimization_plan.csv` | Ordered list |
| Consolidation report | `docs/phase3/consolidation_candidates.md` | Duplicate target analysis |
| Subgraph visualizations | `docs/phase3/subgraphs/` | NetworkX diagrams |

## Workflow Steps

1. **Build Model Dependency Graph** - Parse manifest.json
2. **Collect Query Frequency Metrics** - From QUERY_HISTORY
3. **Collect Credit Consumption Metrics** - From WAREHOUSE_METERING_HISTORY
4. **Calculate Graph Centrality** - PageRank, betweenness, out-degree
5. **Calculate Composite ROI Score** - Weighted average
6. **Assign ROI Tier** - Based on score thresholds
7. **Generate Subgraph Recommendations** - For Tier 1 models
8. **Generate Recommendations** - Clustering, incremental, etc.
9. **HITL Checkpoint** - Tier assignment review
10. **Write ROI Analysis Output**
11. **Target Consolidation Analysis** - Detect duplicate targets

## ROI Score Components

| Dimension | Weight | Source |
|-----------|--------|--------|
| Query Frequency | 30% | QUERY_HISTORY count |
| Bytes Scanned | 25% | GB scanned per query |
| Credit Consumption | 25% | Warehouse credits |
| Centrality | 20% | PageRank + out-degree |

## Tier Classification

| Tier | ROI Score | Optimization Priority |
|------|-----------|----------------------|
| Tier 1 | ≥ 0.70 | High — optimize first |
| Tier 2 | 0.40–0.69 | Medium — optimize after Tier 1 |
| Tier 3 | < 0.40 | Low — only easy wins |

### Special Rules

| Condition | Override |
|-----------|----------|
| Out-degree ≥ 10 | Force Tier 1 |
| Zero query count | Force Tier 3 |
| Staging prefix (`stg_`) | Max Tier 2 |

## Target Consolidation Analysis (v2.0)

Detects multiple models targeting the same table:

```sql
SELECT 
    TARGET_TABLE,
    COUNT(DISTINCT MODEL_NAME) AS model_count,
    ARRAY_AGG(DISTINCT MODEL_NAME) AS models,
    ARRAY_AGG(DISTINCT SOURCE_WORKFLOW) AS source_workflows
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY
GROUP BY TARGET_TABLE, TARGET_SCHEMA
HAVING COUNT(DISTINCT MODEL_NAME) > 1
ORDER BY model_count DESC;
```

### Consolidation Priority

| SQL Similarity | Priority | Recommendation |
|----------------|----------|----------------|
| > 70% | HIGH | MERGE - Models are near-duplicates |
| 40-70% | MEDIUM | REVIEW - Significant overlap |
| < 40% | LOW | KEEP SEPARATE - Distinct logic |

### Similarity Algorithm

Jaccard index on CTE structure (names + logic hashes):

```python
ctes1 = set(extract_cte_names(sql1))
ctes2 = set(extract_cte_names(sql2))
similarity = len(ctes1 & ctes2) / len(ctes1 | ctes2)
```

## Consolidation Output Example

```json
{
  "consolidation_candidates": [
    {
      "target_table": "ANALYTICS.MART_CUSTOMER_360",
      "model_count": 3,
      "models": ["mart_customer_crm", "mart_customer_web", "mart_customer_legacy"],
      "source_workflows": ["wf_crm_load", "wf_web_events", "wf_legacy"],
      "sql_similarity": 0.73,
      "consolidation_priority": "HIGH",
      "recommendation": "MERGE: Models have 73% structural overlap..."
    }
  ]
}
```

## Optimization Recommendations

| Condition | Recommendation |
|-----------|----------------|
| Scan > 100GB | Add cluster_by |
| Query count > 1000 + table materialization | Convert to incremental |
| Out-degree ≥ 5 | Review upstream materializations |

## Error Handling

| Condition | Action |
|-----------|--------|
| INFORMATION_SCHEMA access denied | Hard stop, escalate |
| Query history < 2 cycles | Warning: results unreliable |
| Model not in query history | ROI score = 0, flag for review |
| DAG parsing fails | Use flat list, adjust weights |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| Step 9 | MEDIUM | Tier assignment review before optimization |
