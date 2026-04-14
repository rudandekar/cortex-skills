---
name: live-profiler
version: 0.1.0
tier: project
author: rudandekar
created: 2026-04-07
last_updated: 2026-04-07
status: draft

description: >
  Sub-agent for dbt-model-optimizer. Performs live Snowflake profiling for dimensions
  D5 (SQL performance) and D6 (cost optimization). Requires an active Snowflake
  connection and compiled SQL from dbt. Invoked by the parent orchestrator — do not
  invoke directly.
---

# Live Profiler (Sub-Agent B)

Performs live Snowflake-connected analysis of a compiled dbt model to produce
performance and cost optimization recommendations. Requires an active Snowflake
connection. Returns structured recommendations for the parent orchestrator.

## Inputs

- **Required:** `compiled_sql` — the compiled SQL output from `dbt compile --select {model}`
- **Required:** `model_name` — the dbt model name (used to match query_tag in history)
- **Optional:** `query_tag` — explicit query_tag value (if different from model_name)
- **Optional:** `current_config` — the model's current config block (materialized, cluster_by, etc.)

## Pre-conditions

- Active Snowflake connection (verified by parent orchestrator)
- `SNOWFLAKE.ACCOUNT_USAGE` access for query history analysis
- Compiled SQL available (dbt compile succeeded)

## Workflow

### Step B.1: Execute EXPLAIN plan

Run `EXPLAIN` on the compiled SQL:

```sql
EXPLAIN {compiled_sql}
```

Capture and parse the query plan. Extract:
- Total partitions scanned vs total partitions
- Estimated rows processed
- Join types (hash, merge, nested loop)
- Sort operations
- Pruning effectiveness

### Step B.2: Query execution history

Query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for recent executions:

```sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    TOTAL_ELAPSED_TIME,
    COMPILATION_TIME,
    EXECUTION_TIME,
    QUEUED_PROVISIONING_TIME,
    BYTES_SCANNED,
    BYTES_WRITTEN,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROWS_PRODUCED,
    WAREHOUSE_SIZE,
    CREDITS_USED_CLOUD_SERVICES,
    QUERY_TAG
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TAG ILIKE '%{query_tag}%'
    AND START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND EXECUTION_STATUS = 'SUCCESS'
ORDER BY START_TIME DESC
LIMIT 50;
```

If no results found by query_tag, attempt matching by SQL text pattern (first 200 chars of compiled SQL).

### Step B.3: Analyze performance metrics

From the query history results, compute:

**Spilling analysis:**
- `avg_bytes_spilled_local`: Average bytes spilled to local storage
- `avg_bytes_spilled_remote`: Average bytes spilled to remote storage
- If either > 0: recommend warehouse upsize or query restructure

**Partition pruning analysis:**
- `avg_pruning_ratio`: `1 - (partitions_scanned / partitions_total)`
- If ratio < 0.5: clustering keys may help
- Identify which columns appear in WHERE clauses of the compiled SQL → clustering key candidates

**Execution time analysis:**
- `avg_execution_time_ms`: Average execution time
- `p95_execution_time_ms`: 95th percentile
- `compilation_vs_execution_ratio`: If compilation > 30% of total, model may have excessive Jinja complexity

**Queue time analysis:**
- `avg_queue_time_ms`: If > 5000ms, warehouse may be undersized or overloaded

### Step B.4: Generate clustering recommendations (D5)

If partition pruning ratio < 0.5 OR bytes scanned is high relative to table size:

1. Extract columns from WHERE clauses in the compiled SQL
2. Prioritize: date/timestamp columns first, then high-cardinality filter columns
3. Cap at 4 clustering keys
4. Generate recommendation:

```json
{
  "rule_id": "D5-CLUST-001",
  "dimension": "performance",
  "description": "Add clustering keys to improve partition pruning",
  "current_state": "No clustering keys configured",
  "recommendation": "cluster_by=['transaction_date', 'customer_id']",
  "estimated_impact": "Partition pruning could improve from 30% to 80%+",
  "safety": "REQUIRES_APPROVAL",
  "evidence": {
    "current_pruning_ratio": 0.30,
    "partitions_scanned_avg": 1500,
    "partitions_total_avg": 5000,
    "filter_columns_detected": ["transaction_date", "customer_id", "region"]
  }
}
```

### Step B.5: Generate materialization recommendations (D6)

Analyze query frequency and pattern:

| Current | Condition | Recommendation |
|---------|-----------|----------------|
| `table` | < 10 queries/day, no downstream refs | Downgrade to `view` → save storage costs |
| `table` | Append-only pattern (INSERT, no UPDATE/DELETE) | Convert to `incremental` with `append` strategy |
| `view` | > 100 queries/day, avg execution > 10s | Upgrade to `table` → trade storage for compute |
| `table` | Pure transform, no hooks, stable upstream | Consider `dynamic_table` with appropriate `target_lag` |

Generate recommendation with cost impact estimate:

```json
{
  "rule_id": "D6-MAT-001",
  "dimension": "cost",
  "description": "Downgrade to view — model queried only 3 times/day",
  "current_state": "materialized='table'",
  "recommendation": "materialized='view'",
  "estimated_impact": "Eliminate ~500MB storage, save compute on daily rebuild",
  "safety": "REQUIRES_APPROVAL",
  "evidence": {
    "queries_per_day_avg": 3,
    "downstream_refs": 0,
    "bytes_written_avg": 524288000
  }
}
```

### Step B.6: Generate warehouse sizing recommendations (D6)

If spilling is detected:

```json
{
  "rule_id": "D6-WH-001",
  "dimension": "cost",
  "description": "Upsize warehouse to eliminate spilling",
  "current_state": "Warehouse: SMALL, avg spill: 2.1GB local",
  "recommendation": "Use MEDIUM warehouse for this model's scheduled run",
  "estimated_impact": "Eliminate spilling, reduce execution time ~40%",
  "safety": "REQUIRES_APPROVAL",
  "evidence": {
    "current_warehouse_size": "SMALL",
    "avg_bytes_spilled_local": 2252341248,
    "avg_execution_time_ms": 45000
  }
}
```

If warehouse is underutilized (execution time < 5s, no spilling, warehouse larger than X-SMALL):

```json
{
  "rule_id": "D6-WH-002",
  "dimension": "cost",
  "description": "Downsize warehouse — model runs quickly with no spilling",
  "current_state": "Warehouse: LARGE, avg execution: 3s",
  "recommendation": "Use X-SMALL warehouse for this model",
  "estimated_impact": "Reduce per-query cost by 8x",
  "safety": "REQUIRES_APPROVAL"
}
```

### Step B.7: Return recommendations

Return a JSON array of all D5/D6 recommendations, each with:
- `rule_id`, `dimension`, `description`, `current_state`, `recommendation`
- `estimated_impact`, `safety` (always REQUIRES_APPROVAL), `evidence`

Also return a performance summary:
```json
{
  "model_name": "...",
  "queries_last_30_days": 150,
  "avg_execution_time_ms": 12000,
  "p95_execution_time_ms": 25000,
  "avg_bytes_scanned": 1073741824,
  "avg_bytes_spilled": 0,
  "avg_pruning_ratio": 0.72,
  "warehouse_size": "MEDIUM",
  "recommendations_count": 3
}
```

## Output Specification

| Output | Format | Description |
|--------|--------|-------------|
| Recommendations array | JSON | All D5/D6 recommendations with evidence |
| Performance summary | JSON | Aggregated metrics from query history |

## Error Handling

### No query history found
Report that no matching queries were found in the last 30 days. Skip Steps B.3-B.6. Note that D5/D6 analysis requires at least one historical execution. Recommend the user run the model once and re-run the optimizer.

### ACCOUNT_USAGE access denied
Report the permission error. Note that `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database is required. Skip all D5/D6 analysis.

### EXPLAIN failure
Report the EXPLAIN error. This usually indicates a compilation or permission issue. Skip partition analysis but continue with query history analysis if available.

### Insufficient data points
If fewer than 5 query history records exist, mark all recommendations as lower confidence and note the limited sample size.
