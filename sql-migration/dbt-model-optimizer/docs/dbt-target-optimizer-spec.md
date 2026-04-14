# dbt-target-optimizer — Future Specification

> **Status:** Spec only — not yet implemented. This document captures the design
> intent for the project-level optimization skill that will orchestrate
> `dbt-model-optimizer` across an entire dbt project.

---

## Purpose

`dbt-target-optimizer` operates on a complete dbt project (per-target), executing
Phases 3 and 4 of the six-phase conversion approach and producing Phase 5
recommendations. It provides macro-level cross-model optimization on top of the
micro-level per-model optimization performed by `dbt-model-optimizer`.

## Relationship to Existing Skills

| Skill | Scope | Relationship |
|-------|-------|-------------|
| `dbt-model-optimizer` | Single .sql model | **Invoked by** this skill for per-model optimization |
| `roi-subgraph-scorer` (Agent 6) | DAG scoring | **Absorbed** — Phase 3 scoring logic |
| `dbt-sql-optimizer` (Agent 7) | Config + SQL optimization | **Absorbed** — project-level aspects of Phases 4 and 5 |

## Six-Phase Context

```
Phase 1: Convert & Decompose     → INFA2DBT skill
Phase 2: Lift and Shift          → Orchestrator + manual
Phase 3: Score Subgraphs         → THIS SKILL (automated)
Phase 4: Configuration Optimizations → THIS SKILL (automated)
Phase 5: Targeted Rewrites       → THIS SKILL (report only)
Phase 6: Validate & Govern       → Manual + future skill
```

---

## Phase 3: Score Subgraphs

### Inputs
- `manifest.json` from `dbt compile`
- Snowflake ACCOUNT_USAGE data (query history, credits)

### Workflow

1. **Parse manifest.json** — extract all models, their dependencies, and metadata
2. **Build DAG** — construct the directed acyclic graph of model dependencies
3. **Group by logical target** — identify subgraphs that serve the same business domain
4. **Score each target on 4 dimensions:**

| Dimension | Source | Weight |
|-----------|--------|--------|
| Query frequency | `QUERY_HISTORY.QUERY_TAG` matches | 30% |
| Bytes scanned | `QUERY_HISTORY.BYTES_SCANNED` | 25% |
| Credit consumption | `WAREHOUSE_METERING_HISTORY` | 25% |
| Centrality | DAG in-degree + out-degree | 20% |

5. **Assign tiers:**

| Tier | Criteria | Action |
|------|----------|--------|
| Tier 1 | Top 20% by composite score | Full optimization (Phases 4+5) |
| Tier 2 | Next 30% by composite score | Config optimization (Phase 4) |
| Tier 3 | Bottom 50% by composite score | Recommendations only |

### Output
- Ranked target priority list (JSON + Markdown)
- Tier assignments per model
- Subgraph visualization (Mermaid diagram)

---

## Phase 4: Configuration Optimizations

Applies to ALL tiers (automated for Tier 1+2, recommendations for Tier 3).

### Workflow

1. **Warehouse routing by subgraph weight** — assign heavy subgraphs to dedicated
   warehouses, lightweight subgraphs to shared warehouses
2. **Apply cluster_by on common filter columns** — analyze WHERE clauses across all
   queries that touch each model, recommend clustering on the most frequent filter
   columns (date first, max 4 keys)
3. **Materialization audit** — downgrade infrequently-queried tables to views, upgrade
   hot views to tables, convert eligible models to incremental or dynamic tables
4. **Per-model optimization** — invoke `dbt-model-optimizer` for each model in the
   target, following tier-based scope:
   - Tier 1: All 6 dimensions, auto-apply SAFE changes
   - Tier 2: All 6 dimensions, recommend-only for REVIEW/REQUIRES_APPROVAL
   - Tier 3: D1-D4 only (static analysis), all changes recommended-only
5. **Generate config patches** — produce `dbt_project.yml` updates for warehouse
   routing, clustering, and materialization changes
6. **Validate** — run `dbt compile` + `dbt test` on the entire project
7. **Credit delta validation** — compare estimated credit impact of all changes

### Output
- Updated `dbt_project.yml` (or recommended changes)
- Per-model optimization reports (from `dbt-model-optimizer`)
- Credit delta estimate
- Gate 4 review document

---

## Phase 5: Targeted Rewrites (Report Only)

Applies to Tier 1 and Tier 2 only. This phase produces recommendations — it does
NOT auto-apply changes because these are semantic-changing rewrites.

### Pattern A: Shared Staging Models

**Detection:** Multiple models independently read from the same source table with
different filters or transformations.

**Recommendation:** Create a shared staging model that reads the source once, then
have downstream models ref() the staging model.

```
Before:
  model_a: SELECT ... FROM source('raw', 'orders') WHERE status = 'active'
  model_b: SELECT ... FROM source('raw', 'orders') WHERE status = 'pending'

After:
  stg_orders: SELECT ... FROM source('raw', 'orders')
  model_a: SELECT ... FROM ref('stg_orders') WHERE status = 'active'
  model_b: SELECT ... FROM ref('stg_orders') WHERE status = 'pending'
```

### Pattern B: Shared Intermediate Models

**Detection:** Multiple models perform identical transformations (same CTEs, same
joins, same business logic) independently.

**Recommendation:** Extract the shared logic into an intermediate model and have
both reference it.

### Pattern C: Incremental Consolidation

**Detection:** Separate full-load and incremental-load models exist for the same
target table (e.g., `n_orders_full.sql` and `n_orders_incr.sql`).

**Recommendation:** Consolidate into a single model using `{% if is_incremental() %}`
for the incremental filter, eliminating the full-load model.

```sql
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id') }}

SELECT ...
FROM {{ source('raw', 'orders') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Output
- Phase 5 recommendation report (Markdown)
- Before/after model diagrams for each recommended rewrite
- Estimated credit impact per recommendation

---

## Implementation Notes

### Inputs
- **Required:** Path to dbt project root (directory containing `dbt_project.yml`)
- **Required:** Active Snowflake connection (for Phases 3 and 4)
- **Optional:** Target name (if project has multiple targets in `profiles.yml`)

### Dependencies
- `dbt-model-optimizer` skill (invoked per-model in Phase 4)
- `dbt compile` and `dbt test` (for validation)
- `SNOWFLAKE.ACCOUNT_USAGE` access (for scoring)

### HITL Checkpoints
1. **After Phase 3:** Present tier assignments, ask for approval before proceeding
2. **After Phase 4:** Present all changes, ask for approval before applying to project
3. **Phase 5:** Report-only, no approval needed (user applies manually)

### Estimated scope
- Phase 3 scoring: Pure analysis, no file changes
- Phase 4 config: Modifies `dbt_project.yml` and individual model configs
- Phase 5 rewrites: Report only — user implements recommendations manually

---

*This specification is a placeholder for future implementation. The `dbt-model-optimizer`
skill should be validated and promoted to `status: active` before this skill is built.*
