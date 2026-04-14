---
name: roi-subgraph-scorer
description: >
  Analyzes DAG subgraphs to identify high-ROI optimization targets based on
  query frequency, scan volume, credit consumption, and centrality. Use this
  skill when prioritizing optimization efforts, identifying high-value models,
  analyzing query patterns, assessing credit usage, or generating ROI tier
  classifications. Trigger on keywords: ROI scoring, optimization priority,
  high value models, query frequency, credit consumption, centrality analysis,
  tier classification, Phase 3 analysis.
  This skill runs AFTER Phase 2 stabilization (lift-and-shift complete).
  Do NOT use during Phase 1 — this is a Phase 3 activity.
---

# ROI Subgraph Scorer (Agent 6)

Agent 6 answers the question: **which models should we optimize first?** It runs
after Phase 2 stabilization, when the lift-and-shift models have been running in
production for 2+ business cycles.

**Critical timing:** Agent 6 requires 2+ business cycles of production data to
produce meaningful ROI scores. Running it earlier will produce unreliable results.

## Inputs

- **Required:** `dbt_project_path` — Path to DBT project root
- **Required:** `snowflake_connection` — Connection with access to INFORMATION_SCHEMA
- **Required:** `analysis_window_days` — Days of query history to analyze (default: 30)
- **Optional:** `weight_config` — Custom ROI weights (default: standard weights)

## Pre-conditions

- Phase 2 complete: lift-and-shift models deployed to production
- Minimum 2 business cycles of query history available
- Access to Snowflake INFORMATION_SCHEMA and ACCOUNT_USAGE views

## Workflow

### Step 1: Build model dependency graph

Parse `manifest.json` to build a NetworkX DiGraph of model dependencies.

**Load** references/roi-scoring-algorithms.md for the `build_dbt_dag` implementation.

### Step 2: Collect query frequency metrics

```sql
SELECT
    t.table_name AS model_name,
    COUNT(DISTINCT qh.query_id) AS query_count,
    COUNT(DISTINCT qh.user_name) AS unique_users,
    SUM(qh.bytes_scanned) / 1e9 AS total_gb_scanned,
    AVG(qh.execution_time) / 1000 AS avg_execution_seconds
FROM snowflake.information_schema.tables t
LEFT JOIN snowflake.account_usage.query_history qh
    ON CONTAINS(qh.query_text, t.table_name)
    AND qh.start_time >= DATEADD(day, -{analysis_window_days}, CURRENT_TIMESTAMP())
    AND qh.query_type = 'SELECT'
WHERE t.table_schema = '{dbt_schema}'
GROUP BY t.table_name
ORDER BY query_count DESC;
```

### Step 3: Collect credit consumption metrics

```sql
SELECT
    t.table_name AS model_name,
    SUM(wuh.credits_used) AS total_credits
FROM snowflake.information_schema.tables t
INNER JOIN snowflake.account_usage.query_history qh
    ON CONTAINS(qh.query_text, t.table_name)
    AND qh.start_time >= DATEADD(day, -{analysis_window_days}, CURRENT_TIMESTAMP())
INNER JOIN snowflake.account_usage.warehouse_metering_history wuh
    ON qh.warehouse_name = wuh.warehouse_name
    AND DATE_TRUNC('hour', qh.start_time) = wuh.start_time
WHERE t.table_schema = '{dbt_schema}'
GROUP BY t.table_name
ORDER BY total_credits DESC;
```

### Steps 4-8: Calculate scores and assign tiers

Calculate graph centrality (PageRank + betweenness + out-degree), compute composite ROI score using weighted dimensions, assign tier (1/2/3), identify optimization subgraphs for Tier 1, and generate human-readable recommendations.

**Load** references/roi-scoring-algorithms.md for all scoring and subgraph algorithms.

### Step 9: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Tier assignment review

Present Tier 1 models with ROI scores and recommendations, Tier 2 models with scores, and Tier 3 count. User may approve, override specific tiers, or request re-scoring with adjusted weights.

### Step 10: Write ROI analysis output

Output JSON with: analysis metadata, tier_summary counts, tier_1_models array (with roi_score, query_count, gb_scanned, credits, recommendations), tier_2_models array, tier_3_count, and subgraph_recommendations.

### Step 11: Target Consolidation Analysis

Identify multiple models writing to the same target table. Compare SQL structure similarity (Jaccard on CTE names). Classify as: MERGE (>70%), REVIEW (40-70%), KEEP SEPARATE (<40%).

**Load** references/consolidation-analysis.md for detection query and analysis code.

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| ROI analysis | `docs/phase3/roi_analysis.json` | JSON | Full analysis |
| Tier 1 report | `docs/phase3/tier1_models.md` | Markdown | Human-readable Tier 1 |
| Optimization plan | `docs/phase3/optimization_plan.csv` | CSV | Ordered optimization list |
| Consolidation report | `docs/phase3/consolidation_candidates.md` | Markdown | Duplicate target analysis |

## Error Handling

| Condition | Action |
|-----------|--------|
| INFORMATION_SCHEMA access denied | Hard stop, escalate permission error |
| Query history < 2 business cycles | Warning, generate with `insufficient_history = True` |
| Model not found in query history | Assign ROI score = 0, flag for review |
| DAG parsing fails | Use flat model list, adjust weights to compensate |

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Step 9 | 🟡 MEDIUM | Tier assignment review before optimization |

## Reference Tables

### ROI Weight Defaults

| Dimension | Weight | Description |
|-----------|--------|-------------|
| Query Frequency | 30% | Query count from QUERY_HISTORY |
| Bytes Scanned | 25% | GB scanned per query |
| Credit Consumption | 25% | Warehouse credits attributed to model |
| Centrality | 20% | PageRank + out-degree in DAG |

### Tier Thresholds

| Tier | ROI Score | Optimization Priority |
|------|-----------|----------------------|
| 1 | ≥ 0.70 | High — optimize in Phase 4 |
| 2 | 0.40–0.69 | Medium — optimize after Tier 1 |
| 3 | < 0.40 | Low — only easy wins |

### Special Tier Rules

| Condition | Override |
|-----------|----------|
| Out-degree ≥ 10 | Force Tier 1 |
| Zero query count | Force Tier 3 |
| Staging prefix (`stg_`) | Max Tier 2 |
