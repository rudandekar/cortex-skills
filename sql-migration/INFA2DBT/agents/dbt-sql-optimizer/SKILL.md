---
name: dbt-sql-optimizer
description: >
  Optimizes DBT models through config-level changes (Phase 4) and SQL rewrites
  (Phase 5) based on ROI tier prioritization. Use this skill when optimizing
  model performance, adding clustering, changing materializations, rewriting
  CTEs for efficiency, or implementing Snowflake-specific optimizations. Trigger
  on keywords: optimize dbt, clustering, materialization change, CTE rewrite,
  performance tuning, Snowflake optimization, query profiling, Phase 4, Phase 5.
  This skill has two sub-agents: A (config) and B (SQL).
  Do NOT use for ROI analysis — use roi-subgraph-scorer instead.
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

**Step 7A.1:** Profile current config block from model file.

**Step 7A.2:** Recommend clustering keys — prioritize high-cardinality filter columns, date columns first, max 4 keys.

**Step 7A.3:** Recommend materialization changes:

| Current | Condition | Recommendation |
|---------|-----------|----------------|
| `table` | Query count > 1000/day, upstream changes frequently | `incremental` |
| `table` | Query count < 10/day | Downgrade to `view` |
| `view` | Query count > 100/day, query time > 10s | Upgrade to `table` |
| `ephemeral` | Referenced 3+ times | Upgrade to `view` |

**Step 7A.4:** Generate config patch:

```jinja
{{ config(
    materialized='table',
    cluster_by=['transaction_date', 'customer_id'],
    query_tag='wf_sales_daily',
    schema='mart',
    tags=['wf_sales_daily', 'daily', 'sales'],
    meta={'source_workflow': '...', 'optimized_by': 'Agent_7A'}
) }}
```

**Step 7A.5:** Apply for Tier 1/2, recommend-only for Tier 3.

**Load** references/optimization-patterns.md for all implementation code.

### 7A Output

JSON with: model_name, tier, changes_applied (type, old_value, new_value), estimated_impact.

---

## Sub-Agent 7B: SQL Optimizer (Phase 5)

### Inputs
- **Required:** `model_path` — Path to DBT model SQL file
- **Required:** `roi_tier` — Must be Tier 1 for Pattern C rewrites
- **Required:** `7a_output` — Sub-agent 7A's optimization results

### Optimization Patterns

**Pattern A: CTE Pruning (Automated — Tier 1, 2)** — Remove unused CTEs and consolidate redundant ones (identical SQL body). Safe: no semantic changes.

**Pattern B: Predicate Pushdown (Automated — Tier 1, 2)** — Move filters earlier in the CTE chain to reduce scan volume. Safe: no semantic changes.

**Pattern C: CTE Consolidation (⚠️ HITL — Tier 1 only)** — Combine multiple CTEs reading from same source into a single CTE with combined filter. Example: three CTEs with `WHERE status = 'ACTIVE'`, `WHERE tier = 'PREMIUM'`, and UNION ALL → single CTE with `WHERE status = 'ACTIVE' OR tier = 'PREMIUM'`.

**⚠️ Pattern C changes semantics.** Requires HITL approval: show before/after SQL, explain semantic change, require explicit approval.

### Workflow

**Steps 7B.1-7B.3:** Parse SQL structure, apply Pattern A (prune unused CTEs), apply Pattern B (push filters to source CTEs). All automated for Tier 1/2.

**Step 7B.4: ⚠️ HITL CHECKPOINT — 🔴 HIGH — Pattern C approval.** Present each candidate with before/after SQL and semantic change explanation.

**Step 7B.5:** Apply approved Pattern C changes.

**Step 7B.6:** Validate with `dbt compile --select {model}` and `dbt test --select {model}`. Rollback on failure.

**Load** references/optimization-patterns.md for all implementation code.

### 7B Output

JSON with: model_name, tier, pattern_a/b/c_changes, validation status.

---

## Combined Workflow (Phase 4 → Phase 5)

```
Phase 4 (7A): Profile → Cluster keys → Materialization → Apply → Gate 4
Phase 5 (7B): Pattern A (auto) → Pattern B (auto) → Pattern C (HITL) → Validate → Gate 5
```

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| Config changes | `logs/agent7a/` | JSON | Per-model config patches |
| SQL changes | `logs/agent7b/` | JSON | Per-model SQL changes |
| Phase 4 summary | `docs/phase4/optimization_summary.md` | Markdown | Gate 4 review |
| Phase 5 summary | `docs/phase5/optimization_summary.md` | Markdown | Gate 5 review |

## Error Handling

- **Config change causes compile failure:** Rollback, log, continue.
- **Pattern A/B causes test failure:** Rollback, log, continue.
- **Pattern C rejected:** Log rejection, do not apply, continue.
- **Cluster key exceeds 4:** Warn, truncate to 4, log dropped keys.

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
