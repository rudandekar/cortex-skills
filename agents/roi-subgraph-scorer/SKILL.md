---
name: roi-subgraph-scorer
version: 1.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

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

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md]
---

# ROI Subgraph Scorer (Agent 6)

Agent 6 answers the question: **which models should we optimize first?** It runs
after Phase 2 stabilization, when the lift-and-shift models have been running in
production for 2+ business cycles. This timing ensures the analysis uses real
production query patterns, not estimated ones.

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

```python
import networkx as nx

def build_dbt_dag(project_path):
    G = nx.DiGraph()
    
    # Parse manifest.json for model relationships
    with open(f"{project_path}/target/manifest.json") as f:
        manifest = json.load(f)
    
    for node_id, node in manifest['nodes'].items():
        if node['resource_type'] == 'model':
            G.add_node(node['name'], **node)
            for dep in node.get('depends_on', {}).get('nodes', []):
                if dep.startswith('model.'):
                    dep_name = dep.split('.')[-1]
                    G.add_edge(dep_name, node['name'])
    
    return G

dag = build_dbt_dag(dbt_project_path)
```

### Step 2: Collect query frequency metrics

```sql
-- Query models by downstream query count
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
-- Warehouse credit usage attributed to model queries
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

### Step 4: Calculate graph centrality scores

```python
def calculate_centrality_scores(dag):
    scores = {}
    
    # PageRank: importance based on incoming references
    pagerank = nx.pagerank(dag)
    
    # Betweenness: bridges between subgraphs
    betweenness = nx.betweenness_centrality(dag)
    
    # Out-degree: downstream dependents
    out_degree = dict(dag.out_degree())
    
    for node in dag.nodes():
        scores[node] = {
            'pagerank': pagerank.get(node, 0),
            'betweenness': betweenness.get(node, 0),
            'out_degree': out_degree.get(node, 0)
        }
    
    return scores

centrality = calculate_centrality_scores(dag)
```

### Step 5: Calculate composite ROI score

```python
def calculate_roi_score(model_name, query_metrics, credit_metrics, centrality_scores, weights):
    # Default weights
    w = weights or {
        'query_frequency': 0.30,
        'bytes_scanned': 0.25,
        'credit_consumption': 0.25,
        'centrality': 0.20
    }
    
    # Normalize to 0-1 scale (within the population)
    qm = query_metrics.get(model_name, {})
    cm = credit_metrics.get(model_name, {})
    cs = centrality_scores.get(model_name, {})
    
    # Calculate weighted score
    score = (
        w['query_frequency'] * normalize(qm.get('query_count', 0), all_query_counts) +
        w['bytes_scanned'] * normalize(qm.get('total_gb_scanned', 0), all_scan_volumes) +
        w['credit_consumption'] * normalize(cm.get('total_credits', 0), all_credits) +
        w['centrality'] * normalize(cs.get('pagerank', 0) + cs.get('out_degree', 0), all_centrality)
    )
    
    return score  # 0.0 to 1.0
```

### Step 6: Assign ROI tier

```python
def assign_tier(roi_score, model_name, dag):
    # Special cases
    if dag.out_degree(model_name) >= 10:
        return 'tier_1'  # High-fanout nodes always Tier 1
    
    # Standard tiering
    if roi_score >= 0.70:
        return 'tier_1'  # High value — optimize first
    elif roi_score >= 0.40:
        return 'tier_2'  # Medium value — optimize after Tier 1
    else:
        return 'tier_3'  # Low value — optimize only if easy wins
```

### Step 7: Generate subgraph recommendations

For Tier 1 models, identify optimization subgraphs:

```python
def identify_optimization_subgraph(model_name, dag, tier_assignments):
    # Get upstream models (potential source of issues)
    upstream = list(nx.ancestors(dag, model_name))
    
    # Get downstream models (impact radius)
    downstream = list(nx.descendants(dag, model_name))
    
    # Identify the subgraph to optimize together
    optimization_group = [model_name]
    
    # Include upstream Tier 1/2 models
    for u in upstream:
        if tier_assignments[u] in ['tier_1', 'tier_2']:
            optimization_group.append(u)
    
    return {
        'anchor_model': model_name,
        'optimization_group': optimization_group,
        'impact_radius': len(downstream),
        'recommendation': generate_recommendation(model_name, dag)
    }
```

### Step 8: Generate human-readable recommendations

```python
def generate_recommendation(model_name, metrics, dag):
    recs = []
    
    # High scan volume → clustering
    if metrics['total_gb_scanned'] > 100:
        recs.append({
            'type': 'clustering',
            'priority': 'high',
            'description': f"Add cluster_by based on common filter columns",
            'estimated_impact': 'Reduce scan volume by 40-60%'
        })
    
    # High query frequency + table materialization → consider incremental
    if metrics['query_count'] > 1000 and dag.nodes[model_name].get('materialized') == 'table':
        recs.append({
            'type': 'incremental',
            'priority': 'medium',
            'description': 'Convert to incremental materialization',
            'estimated_impact': 'Reduce refresh time and cost'
        })
    
    # High out-degree → ensure ephemeral upstream
    if dag.out_degree(model_name) >= 5:
        upstream_tables = [u for u in dag.predecessors(model_name) 
                         if dag.nodes[u].get('materialized') == 'table']
        if upstream_tables:
            recs.append({
                'type': 'materialization_review',
                'priority': 'medium',
                'description': f'Review upstream materializations: {upstream_tables}',
                'estimated_impact': 'Reduce refresh costs'
            })
    
    return recs
```

### Step 9: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Tier assignment review

Present to the user:
- Tier 1 models (N): List with ROI scores and recommendations
- Tier 2 models (M): List with ROI scores
- Tier 3 models (P): Count only (not individually listed)
- Subgraph recommendations for Tier 1

**Required response:** Confirmation of tier assignments before Agent 7 proceeds.
User may:
- Approve all tiers
- Override specific model tiers
- Request re-scoring with adjusted weights

### Step 10: Write ROI analysis output

```json
{
  "analysis_timestamp": "2026-03-15T14:00:00Z",
  "analysis_window_days": 30,
  "models_analyzed": 142,
  
  "tier_summary": {
    "tier_1": 18,
    "tier_2": 47,
    "tier_3": 77
  },
  
  "tier_1_models": [
    {
      "model_name": "mart_sales_summary",
      "roi_score": 0.92,
      "query_count": 4521,
      "total_gb_scanned": 847.3,
      "total_credits": 124.5,
      "centrality_score": 0.34,
      "recommendations": [
        {"type": "clustering", "priority": "high"},
        {"type": "incremental", "priority": "medium"}
      ]
    },
    ...
  ],
  
  "tier_2_models": [...],
  
  "tier_3_count": 77,
  
  "subgraph_recommendations": [
    {
      "anchor_model": "mart_sales_summary",
      "optimization_group": ["stg_sales_raw", "int_sales_transformed", "mart_sales_summary"],
      "impact_radius": 12,
      "total_group_credits": 187.3
    }
  ]
}
```

### Step 11: Target Consolidation Analysis

Identify multiple models writing to the same target table — a key optimization opportunity.

```sql
-- Detect consolidation candidates from MODEL_REGISTRY
SELECT 
    TARGET_TABLE,
    TARGET_SCHEMA,
    COUNT(DISTINCT MODEL_NAME) AS model_count,
    ARRAY_AGG(DISTINCT MODEL_NAME) AS models,
    ARRAY_AGG(DISTINCT SOURCE_WORKFLOW) AS source_workflows,
    SUM(CTE_COUNT) AS total_ctes,
    AVG(COLUMN_COUNT) AS avg_columns
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY
GROUP BY TARGET_TABLE, TARGET_SCHEMA
HAVING COUNT(DISTINCT MODEL_NAME) > 1
ORDER BY model_count DESC;
```

```python
def analyze_consolidation_candidates(conn, dag):
    """Identify models targeting same table and assess consolidation potential."""
    cursor = conn.cursor()
    cursor.execute(CONSOLIDATION_QUERY)
    
    candidates = []
    for row in cursor.fetchall():
        target_table = row[0]
        models = row[3]  # Array of model names
        workflows = row[4]  # Source workflows
        
        # Compare SQL patterns between models
        similarity_scores = []
        for i, m1 in enumerate(models):
            for m2 in models[i+1:]:
                sim = compare_model_sql(conn, m1, m2)
                similarity_scores.append(sim)
        
        avg_similarity = sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0
        
        candidates.append({
            'target_table': f"{row[1]}.{target_table}",
            'model_count': row[2],
            'models': models,
            'source_workflows': workflows,
            'sql_similarity': avg_similarity,
            'consolidation_priority': 'HIGH' if avg_similarity > 0.7 else 'MEDIUM' if avg_similarity > 0.4 else 'LOW',
            'recommendation': generate_consolidation_recommendation(models, avg_similarity, workflows)
        })
    
    return sorted(candidates, key=lambda x: (-x['model_count'], -x['sql_similarity']))


def compare_model_sql(conn, model1: str, model2: str) -> float:
    """Compare SQL content similarity between two models (0.0 to 1.0)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SQL_CONTENT FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY 
        WHERE MODEL_NAME IN (%s, %s)
    """, (model1, model2))
    
    results = cursor.fetchall()
    if len(results) != 2:
        return 0.0
    
    sql1, sql2 = results[0][0], results[1][0]
    
    # Extract CTE names and compare structure
    ctes1 = set(re.findall(r'(\w+)\s+AS\s*\(', sql1, re.IGNORECASE))
    ctes2 = set(re.findall(r'(\w+)\s+AS\s*\(', sql2, re.IGNORECASE))
    
    if not ctes1 and not ctes2:
        return 0.0
    
    # Jaccard similarity of CTE structure
    intersection = len(ctes1 & ctes2)
    union = len(ctes1 | ctes2)
    
    return intersection / union if union > 0 else 0.0


def generate_consolidation_recommendation(models: list, similarity: float, workflows: list) -> str:
    """Generate human-readable consolidation recommendation."""
    if similarity > 0.7:
        return f"MERGE: Models have {similarity:.0%} structural overlap. Likely duplicate logic from workflows: {', '.join(workflows)}. Recommend merging into single model."
    elif similarity > 0.4:
        return f"REVIEW: Models have {similarity:.0%} overlap. May have shared transformations. Review for partial consolidation."
    else:
        return f"KEEP SEPARATE: Low similarity ({similarity:.0%}). Models likely serve different purposes despite same target."
```

Add consolidation candidates to output:

```json
{
  "consolidation_candidates": [
    {
      "target_table": "ANALYTICS.MART_CUSTOMER_360",
      "model_count": 3,
      "models": ["mart_customer_crm", "mart_customer_web", "mart_customer_legacy"],
      "source_workflows": ["wf_crm_load", "wf_web_events", "wf_legacy_migration"],
      "sql_similarity": 0.73,
      "consolidation_priority": "HIGH",
      "recommendation": "MERGE: Models have 73% structural overlap..."
    }
  ],
  "estimated_consolidation_savings": {
    "models_reducible": 12,
    "estimated_credit_savings_pct": 15
  }
}
```

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| ROI analysis | `docs/phase3/roi_analysis.json` | JSON | Full analysis |
| Tier 1 report | `docs/phase3/tier1_models.md` | Markdown | Human-readable Tier 1 |
| Optimization plan | `docs/phase3/optimization_plan.csv` | CSV | Ordered optimization list |
| Consolidation report | `docs/phase3/consolidation_candidates.md` | Markdown | Duplicate target analysis |
| Subgraph viz | `docs/phase3/subgraphs/` | PNG/HTML | NetworkX visualizations |

Inline summary must include:
- Models analyzed: N
- Tier 1 (high ROI): N models
- Tier 2 (medium ROI): N models
- Tier 3 (low ROI): N models
- Consolidation candidates: N target tables with M duplicate models
- Top 5 Tier 1 models with scores
- Total estimated credit savings potential

## Error Handling

### INFORMATION_SCHEMA access denied
Hard stop. ROI scoring requires access to ACCOUNT_USAGE views. Log permission
error and escalate.

### Query history insufficient (< 2 business cycles)
Warning: results may be unreliable. Generate analysis with caveat flag. Include
`insufficient_history = True` in output.

### Model not found in query history
Assign ROI score = 0. These models may be unused or only used in batch jobs.
Flag for review.

### DAG parsing fails
Use flat model list without centrality scoring. Adjust weights to compensate
(increase query_frequency and credit_consumption weights).

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Step 9 | 🟡 MEDIUM | Tier assignment review before optimization |

## Reference Tables

### ROI Weight Defaults (4 Dimensions)

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
