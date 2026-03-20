# Agent 7: dbt SQL Optimizer

**Skill Name:** `dbt-sql-optimizer`  
**Version:** 1.0.0  
**Phase:** 4-5 (Optimization)

## Purpose

Optimizes dbt models in two phases:
- **Sub-agent 7A (Phase 4):** Config-level changes — no SQL rewrites
- **Sub-agent 7B (Phase 5):** SQL refactoring — logic rewrites

Uses ROI tier assignments from Agent 6 to prioritize work.

## Sub-Agent 7A: Config Optimizer (Phase 4)

### Inputs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_path` | Yes | Path to dbt model SQL file |
| `roi_tier` | Yes | Tier assignment from Agent 6 |
| `query_patterns` | Yes | Filter columns, join keys from Agent 6 |

### Config Changes

#### Clustering Key Recommendations

```python
# Prioritize: 1) High-cardinality filters, 2) Date columns, 3) Join keys
recommended_keys = date_cols[:1] + other_cols[:3]  # Max 4 keys
```

#### Materialization Changes

| Current | Condition | Recommendation |
|---------|-----------|----------------|
| `table` | Query count > 1000/day, upstream changes | `incremental` |
| `table` | Query count < 10/day | Downgrade to `view` |
| `view` | Query count > 100/day, time > 10s | Upgrade to `table` |
| `ephemeral` | Referenced 3+ times | Upgrade to `view` |

### Tier-Based Application

| Tier | Action |
|------|--------|
| Tier 1, 2 | Apply config changes directly |
| Tier 3 | Generate recommendations only |

### 7A Output

```json
{
  "model_name": "mart_sales_summary",
  "tier": "tier_1",
  "changes_applied": [
    {"type": "cluster_by", "new_value": ["transaction_date", "customer_id"]},
    {"type": "materialization", "old": "table", "new": "incremental"}
  ],
  "estimated_impact": {
    "scan_reduction_pct": 45,
    "credit_savings_monthly": 12.5
  }
}
```

---

## Sub-Agent 7B: SQL Optimizer (Phase 5)

### Optimization Patterns

#### Pattern A: CTE Pruning (Automated)
- Remove unused CTEs
- Consolidate redundant CTEs (identical SQL body)
- **Applies to:** Tier 1, 2

#### Pattern B: Predicate Pushdown (Automated)
- Move filters earlier in CTE chain
- Push filters to source CTEs where possible
- **Applies to:** Tier 1, 2

#### Pattern C: CTE Consolidation (HITL Required)
- Combine multiple CTEs with same source
- **Changes semantics** — requires approval
- **Applies to:** Tier 1 only

### Pattern C Example

**Before:**
```sql
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
```

**After:**
```sql
WITH filtered_customers AS (
    SELECT * FROM source_customers
    WHERE status = 'ACTIVE' OR tier = 'PREMIUM'
)
```

**Semantic Change:** UNION ALL → OR filter (may affect duplicate handling)

### Pattern Safety Matrix

| Pattern | Semantic Change | Auto-Apply | HITL Required |
|---------|-----------------|------------|---------------|
| A: CTE Prune | No | Tier 1, 2 | No |
| B: Predicate Push | No | Tier 1, 2 | No |
| C: CTE Consolidate | Yes | Tier 1 only | Yes |

### Validation

After all changes:
```bash
dbt compile --select {model_name}
dbt test --select {model_name}
```

- Compile fails → Rollback changes
- Test fails → Rollback changes, flag for review

### 7B Output

```json
{
  "model_name": "mart_sales_summary",
  "pattern_a_changes": [{"cte_removed": "unused_staging_cte"}],
  "pattern_b_changes": [{"filter_pushed": "status_filter", "from": "final", "to": "source"}],
  "pattern_c_changes": [{"id": "c_001", "approved": true}],
  "validation": {"compile": "passed", "tests": "passed"},
  "estimated_impact": {"scan_reduction_pct": 38}
}
```

---

## Combined Workflow

```
PHASE 4: Sub-Agent 7A (Config Optimization)
  ├── Profile current config
  ├── Recommend clustering keys
  ├── Recommend materialization changes
  ├── Apply for Tier 1 & 2, recommend for Tier 3
  └── Gate 4: Config changes validation (HITL)

PHASE 5: Sub-Agent 7B (SQL Optimization)
  ├── Pattern A: CTE Pruning (auto, Tier 1-2)
  ├── Pattern B: Predicate Pushdown (auto, Tier 1-2)
  ├── Pattern C: CTE Consolidation (Tier 1 only)
  │     └── Gate 5: HITL approval per Pattern C
  └── Validate optimized models
```

## Tier-Based Optimization Scope

| Tier | Phase 4 (7A) | Phase 5 (7B) |
|------|--------------|--------------|
| 1 | Full optimization | Full + Pattern C |
| 2 | Full optimization | Patterns A & B only |
| 3 | Recommendations only | Recommendations only |

## Error Handling

| Condition | Action |
|-----------|--------|
| Config change causes compile failure | Rollback, log, continue |
| Pattern A/B causes test failure | Rollback, log, continue |
| Pattern C rejected | Log rejection, continue |
| Cluster key > 4 | Truncate to 4, log dropped keys |

## HITL Checkpoints

| Step | Risk Level | Approval Required |
|------|-----------|-------------------|
| Gate 4 | MEDIUM | Config changes validation |
| Gate 5 / Step 7B.4 | HIGH | Each Pattern C rewrite |
