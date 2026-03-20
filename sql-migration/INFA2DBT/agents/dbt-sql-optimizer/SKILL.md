---
name: dbt-sql-optimizer
version: 1.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-15
last_updated: 2026-03-15
status: active

description: >
  Optimizes DBT models through config-level changes (Phase 4) and SQL rewrites
  (Phase 5) based on ROI tier prioritization. Use this skill when optimizing
  model performance, adding clustering, changing materializations, rewriting
  CTEs for efficiency, or implementing Snowflake-specific optimizations. Trigger
  on keywords: optimize dbt, clustering, materialization change, CTE rewrite,
  performance tuning, Snowflake optimization, query profiling, Phase 4, Phase 5.
  This skill has two sub-agents: A (config) and B (SQL).
  Do NOT use for ROI analysis — use roi-subgraph-scorer instead.

compatibility:
  tools: [bash, read, write, snowflake_sql_execute]
  context: [CLAUDE.md, PROJECT.md]
---

# DBT SQL Optimizer (Agent 7)

Agent 7 optimizes DBT models in two phases:
- **Sub-agent 7A (Phase 4):** Config-level changes — no SQL rewrites
- **Sub-agent 7B (Phase 5):** SQL refactoring — logic rewrites

Both sub-agents use the ROI tier assignments from Agent 6 to prioritize work.
Tier 1 models are optimized first, then Tier 2, with Tier 3 receiving only
automated quick wins.

**Critical safety:** Sub-agent 7B performs semantic-changing rewrites and requires
HITL approval for each Pattern C optimization. Sub-agent 7A operates autonomously.

## Sub-Agent 7A: Config Optimizer (Phase 4)

### Inputs
- **Required:** `model_path` — Path to DBT model SQL file
- **Required:** `roi_tier` — Tier assignment from Agent 6
- **Required:** `query_patterns` — From Agent 6 analysis (filter columns, join keys)

### Workflow

#### Step 7A.1: Profile current config

```python
def extract_current_config(model_path):
    with open(model_path) as f:
        content = f.read()
    
    # Parse Jinja config block
    config_match = re.search(r'\{\{\s*config\((.*?)\)\s*\}\}', content, re.DOTALL)
    if config_match:
        return parse_config_dict(config_match.group(1))
    return {}
```

#### Step 7A.2: Recommend clustering keys

```python
def recommend_cluster_keys(model_name, query_patterns):
    # Identify most-used filter columns
    filter_columns = query_patterns.get('filter_columns', [])
    
    # Identify join keys
    join_keys = query_patterns.get('join_keys', [])
    
    # Prioritize: 1) High-cardinality filters, 2) Date columns, 3) Join keys
    candidate_keys = []
    for col in filter_columns:
        if col['selectivity'] > 0.01:  # High enough cardinality
            candidate_keys.append(col['column_name'])
    
    # Date columns first (most common filter pattern)
    date_cols = [c for c in candidate_keys if 'date' in c.lower()]
    other_cols = [c for c in candidate_keys if c not in date_cols]
    
    recommended = date_cols[:1] + other_cols[:3]  # Max 4 cluster keys
    return recommended
```

#### Step 7A.3: Recommend materialization changes

| Current | Condition | Recommendation |
|---------|-----------|----------------|
| `table` | Query count > 1000/day, upstream sources stable | Keep `table` |
| `table` | Query count > 1000/day, upstream changes frequently | `incremental` |
| `table` | Query count < 10/day | Downgrade to `view` |
| `view` | Query count > 100/day, query time > 10s | Upgrade to `table` |
| `ephemeral` | Referenced 3+ times | Upgrade to `view` |
| `incremental` | No clear update column | Warn, keep as-is |

#### Step 7A.4: Generate config patch

```jinja
{{ config(
    materialized='table',
    cluster_by=['transaction_date', 'customer_id'],
    query_tag='wf_sales_daily',
    schema='mart',
    tags=['wf_sales_daily', 'daily', 'sales'],
    meta={
        'source_workflow': 'WF_SALES_DAILY',
        'generated_by': 'INFA2DBT_accelerator_v1.1',
        'optimized_by': 'Agent_7A',
        'optimization_timestamp': '2026-03-15T18:30:00Z'
    }
) }}
```

#### Step 7A.5: Apply changes

For Tier 1 and Tier 2: Apply config changes directly.
For Tier 3: Generate recommendations only, do not apply.

```python
def apply_config_changes(model_path, new_config, tier):
    if tier in ['tier_1', 'tier_2']:
        # Apply automatically
        update_model_config(model_path, new_config)
        log_config_change(model_path, 'applied')
    else:
        # Tier 3: recommend only
        save_recommendation(model_path, new_config)
        log_config_change(model_path, 'recommended_only')
```

### 7A Output

```json
{
  "model_name": "mart_sales_summary",
  "tier": "tier_1",
  "changes_applied": [
    {
      "type": "cluster_by",
      "old_value": null,
      "new_value": ["transaction_date", "customer_id"]
    },
    {
      "type": "materialization",
      "old_value": "table",
      "new_value": "incremental"
    }
  ],
  "estimated_impact": {
    "scan_reduction_pct": 45,
    "credit_savings_monthly": 12.5
  }
}
```

---

## Sub-Agent 7B: SQL Optimizer (Phase 5)

### Inputs
- **Required:** `model_path` — Path to DBT model SQL file
- **Required:** `roi_tier` — Must be Tier 1 for Pattern C rewrites
- **Required:** `7a_output` — Sub-agent 7A's optimization results

### Optimization Patterns

#### Pattern A: CTE Pruning (Automated — Tier 1, 2)

Remove unused CTEs and consolidate redundant ones:

```python
def identify_unused_ctes(model_content):
    ctes = extract_all_ctes(model_content)
    referenced_ctes = find_cte_references(model_content)
    
    unused = [cte for cte in ctes if cte['name'] not in referenced_ctes]
    return unused

def identify_redundant_ctes(model_content):
    ctes = extract_all_ctes(model_content)
    # Find CTEs with identical SQL body (ignoring whitespace)
    cte_hashes = {}
    for cte in ctes:
        h = hash_sql_body(cte['body'])
        if h in cte_hashes:
            cte_hashes[h].append(cte['name'])
        else:
            cte_hashes[h] = [cte['name']]
    return {h: names for h, names in cte_hashes.items() if len(names) > 1}
```

**Safe to remove:** CTEs with zero downstream references.
**Safe to consolidate:** CTEs with identical SQL body.
**Apply:** Automatically for Tier 1 and 2.

#### Pattern B: Predicate Pushdown (Automated — Tier 1, 2)

Move filters earlier in the CTE chain:

```python
def identify_pushdown_opportunities(model_content):
    opportunities = []
    
    # Find filters that could be pushed to source CTE
    for cte in ctes:
        if cte['has_filter']:
            filter_col = cte['filter_column']
            # Check if filter could be applied earlier
            source_cte = find_source_cte(cte, filter_col)
            if source_cte and not source_cte['has_filter_on_col'](filter_col):
                opportunities.append({
                    'cte': cte['name'],
                    'filter': cte['filter_expression'],
                    'push_to': source_cte['name']
                })
    
    return opportunities
```

**Apply:** Automatically for Tier 1 and 2.

#### Pattern C: CTE Consolidation (⚠️ HITL — Tier 1 only)

Combine multiple CTEs with same source:

```sql
-- Before: 3 CTEs reading from same source
WITH active_customers AS (
    SELECT * FROM source_customers WHERE status = 'ACTIVE'
),
premium_customers AS (
    SELECT * FROM source_customers WHERE tier = 'PREMIUM'
),
combined AS (
    SELECT * FROM active_customers
    UNION ALL
    SELECT * FROM premium_customers
)

-- After: Single CTE with combined filter
WITH filtered_customers AS (
    SELECT * FROM source_customers
    WHERE status = 'ACTIVE' OR tier = 'PREMIUM'
)
```

**⚠️ Pattern C changes semantics.** Requires HITL approval:
- Show before/after SQL
- Explain semantic change
- Require explicit approval

### Workflow

#### Step 7B.1: Parse SQL structure

```python
def parse_sql_structure(model_path):
    with open(model_path) as f:
        content = f.read()
    
    structure = {
        'config_block': extract_config(content),
        'ctes': extract_ctes(content),
        'final_select': extract_final(content),
        'cte_references': build_cte_reference_graph(content)
    }
    
    return structure
```

#### Step 7B.2: Apply Pattern A (CTE Pruning)

```python
def apply_pattern_a(model_path, structure, tier):
    if tier not in ['tier_1', 'tier_2']:
        return []  # Skip for Tier 3
    
    unused_ctes = identify_unused_ctes(structure)
    
    for cte in unused_ctes:
        remove_cte(model_path, cte['name'])
        log_change('pattern_a', cte['name'], 'removed')
    
    return unused_ctes
```

#### Step 7B.3: Apply Pattern B (Predicate Pushdown)

```python
def apply_pattern_b(model_path, structure, tier):
    if tier not in ['tier_1', 'tier_2']:
        return []  # Skip for Tier 3
    
    opportunities = identify_pushdown_opportunities(structure)
    
    for opp in opportunities:
        move_filter_to_source(model_path, opp)
        log_change('pattern_b', opp['cte'], 'filter_pushed_to', opp['push_to'])
    
    return opportunities
```

#### Step 7B.4: ⚠️ HITL CHECKPOINT — 🔴 HIGH — Pattern C approval

For Tier 1 models only, present Pattern C candidates:

```markdown
## Pattern C Optimization Candidate

**Model:** mart_sales_summary
**Tier:** tier_1

### Before
```sql
WITH active_customers AS (...),
premium_customers AS (...),
combined AS (...)
```

### After
```sql
WITH filtered_customers AS (...)
```

### Semantic Change
This consolidation changes the UNION ALL to an OR filter.
- Before: Returns duplicate rows if customer is both ACTIVE and PREMIUM
- After: Returns single row per customer matching either condition

### Impact
- Estimated 23% scan reduction
- Estimated 3.2 credit savings/month

### Approval Required
- [ ] Approve this semantic change
- [ ] Reject — keep original structure
```

**Required response:** Explicit approval or rejection for EACH Pattern C candidate.

#### Step 7B.5: Apply approved Pattern C changes

```python
def apply_pattern_c(model_path, approved_changes):
    for change in approved_changes:
        if change['approved']:
            apply_cte_consolidation(model_path, change)
            log_change('pattern_c', change['id'], 'applied_with_approval')
        else:
            log_change('pattern_c', change['id'], 'rejected')
```

#### Step 7B.6: Validate optimized model

```bash
dbt compile --select {model_name}
dbt test --select {model_name}
```

**If compile fails:** Rollback changes. Log as optimization failure.
**If test fails:** Rollback changes. Flag for manual review.

### 7B Output

```json
{
  "model_name": "mart_sales_summary",
  "tier": "tier_1",
  "pattern_a_changes": [
    {"cte_removed": "unused_staging_cte"}
  ],
  "pattern_b_changes": [
    {"filter_pushed": "status_filter", "from": "final", "to": "source_customers"}
  ],
  "pattern_c_changes": [
    {"id": "c_001", "approved": true, "ctes_consolidated": ["active", "premium"]}
  ],
  "validation": {
    "compile": "passed",
    "tests": "passed"
  },
  "estimated_impact": {
    "scan_reduction_pct": 38,
    "credit_savings_monthly": 8.7
  }
}
```

---

## Combined Workflow (Phase 4 → Phase 5)

```
┌──────────────────────────────────────────────────────────────┐
│                     Agent 7 Workflow                         │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  PHASE 4: Sub-Agent 7A (Config Optimization)                 │
│    ├── Profile current config                                │
│    ├── Recommend clustering keys                             │
│    ├── Recommend materialization changes                     │
│    ├── Apply for Tier 1 & 2, recommend for Tier 3            │
│    └── ⚠️ Gate 4: Config changes validation (HITL)           │
│                                                              │
│  PHASE 5: Sub-Agent 7B (SQL Optimization)                    │
│    ├── Pattern A: CTE Pruning (auto, Tier 1-2)               │
│    ├── Pattern B: Predicate Pushdown (auto, Tier 1-2)        │
│    ├── Pattern C: CTE Consolidation (Tier 1 only)            │
│    │     └── ⚠️ Gate 5: HITL approval per Pattern C          │
│    └── Validate optimized models                             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| Config changes | `logs/agent7a/` | JSON | Per-model config patches |
| SQL changes | `logs/agent7b/` | JSON | Per-model SQL changes |
| Phase 4 summary | `docs/phase4/optimization_summary.md` | Markdown | Gate 4 review |
| Phase 5 summary | `docs/phase5/optimization_summary.md` | Markdown | Gate 5 review |
| Impact report | `docs/phase5/impact_report.csv` | CSV | Estimated savings |

## Error Handling

### Config change causes compile failure
Rollback config. Log error. Continue with next model.

### Pattern A/B causes test failure
Rollback change. Log as failed optimization. Continue.

### Pattern C rejected
Log rejection. Do not apply. Continue to next candidate.

### Cluster key exceeds Snowflake limit (4 keys)
Warn and truncate to 4. Log which keys were dropped.

## HITL Checkpoints Summary

| Step | Risk Level | What Requires Approval |
|------|-----------|------------------------|
| Gate 4 | 🟡 MEDIUM | Config changes validation |
| Gate 5 / Step 7B.4 | 🔴 HIGH | Each Pattern C rewrite |

## Reference Tables

### Optimization Pattern Safety

| Pattern | Semantic Change | Auto-Apply Tiers | HITL Required |
|---------|-----------------|------------------|---------------|
| A: CTE Prune | No | 1, 2 | No |
| B: Predicate Push | No | 1, 2 | No |
| C: CTE Consolidate | Yes | 1 only | Yes |

### Tier-Based Optimization Scope

| Tier | Phase 4 (7A) | Phase 5 (7B) |
|------|--------------|--------------|
| 1 | Full optimization | Full optimization + Pattern C |
| 2 | Full optimization | Patterns A & B only |
| 3 | Recommendations only | Recommendations only |
