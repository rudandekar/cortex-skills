# Snowflake Optimization Rules

Reference for the `sybase-to-snowflake` skill v0.3.0. Step 12 of the conversion workflow. Each rule
defines detection logic, DDL templates, severity classification, and applicability
conditions. Rules are evaluated per-object against the converted DDL/DML and
metadata from earlier pipeline steps (object_inventory, dependency_graph,
complexity_report).

## Rule Index

| Rule ID | Category | Default Severity | Classification |
|---------|----------|-----------------|----------------|
| O1 | Clustering Keys | HIGH | auto-apply |
| O2 | Transient Tables | HIGH | auto-apply |
| O3 | Search Optimization | MEDIUM | manual-review |
| O4 | Query Acceleration | LOW | manual-review |
| O5 | Informational Constraints | HIGH | auto-apply |
| O6 | Dynamic Table Candidates | MEDIUM | manual-review |
| O7 | Streams + Tasks Roadmap | LOW | manual-review |
| O8 | Storage Tuning | MEDIUM | auto-apply |

---

## O1: Clustering Keys

### When to recommend
- Table type is **fact** (prefix `fact_`) and has at least one date/timestamp
  dimension key column (e.g., `date_key`, `policy_date_key`, `claim_date_key`).
- Table type is **dimension** with high expected row count (>10M rows based on
  source metadata or complexity indicators) and a natural sort column.
- Table has columns frequently used in `WHERE`, `JOIN`, or `GROUP BY` clauses
  in downstream DML/views (cross-reference with dependency_graph.json).

### When NOT to recommend
- Staging tables (transient, truncated each load — clustering is wasted).
- Tables with fewer than ~1 billion micro-partitions (note as "conditional on
  production volume" rather than skipping entirely).
- Tables where all queries are full-scan aggregations with no filters.

### Column selection logic
1. Start with the primary date dimension key (lowest cardinality date column).
2. Add the most common filter/join column from downstream queries.
3. Maximum 3–4 columns, ordered low-to-high cardinality.

### DDL template
```sql
-- O1: Clustering key for range queries on date dimension keys
-- Ref: https://docs.snowflake.com/en/user-guide/tables-clustering-keys
ALTER TABLE {schema}.{table_name}
  CLUSTER BY ({col1}, {col2});
```

### Severity rules
- Fact table with date key + downstream range filters → **HIGH**
- Large dimension with natural sort column → **MEDIUM**
- Any table where benefit is conditional on production volume → append
  "(conditional on table size exceeding ~1TB)" to rationale

### Classification
`auto-apply` — clustering key is additive metadata; does not alter data or schema.
Automatic Clustering maintains the ordering in the background.

---

## O2: Transient Tables

### When to recommend
- Table is in the **staging** layer (prefix `stg_` or located in staging DDL file).
- DML pattern shows `TRUNCATE` or `DELETE` before `INSERT` (full reload pattern).
- Table is not referenced by Time Travel queries or has no downstream need for
  Fail-safe recovery.

### When NOT to recommend
- Dimension or fact tables (need Time Travel for operational recovery).
- Audit/log tables (need Fail-safe for compliance).
- Any table the user explicitly marks as requiring full data protection.

### DDL template
```sql
-- O2: Transient table for staging (no Fail-safe, reduces storage cost)
-- Ref: https://docs.snowflake.com/en/user-guide/tables-temp-transient
CREATE OR REPLACE TRANSIENT TABLE {schema}.{table_name} (
  {column_definitions}
);
```

### Detection logic
1. Scan DDL files for `CREATE OR REPLACE TABLE` statements where the table name
   matches staging patterns (`stg_*`, or file is `ddl_staging.sql`).
2. Confirm DML shows truncate-and-reload pattern.
3. Emit recommendation to change `CREATE OR REPLACE TABLE` →
   `CREATE OR REPLACE TRANSIENT TABLE`.

### Severity rules
- Staging table with confirmed truncate-reload pattern → **HIGH**
- Staging table without clear reload pattern → **MEDIUM**

### Classification
`auto-apply` — adding `TRANSIENT` keyword is safe and reduces storage cost.
The only trade-off is loss of Fail-safe (0 days instead of 7), which is
acceptable for transitory staging data.

---

## O3: Search Optimization Service

### When to recommend
- Table has high-cardinality columns used in equality predicates (`=`, `IN`):
  policy_number, claim_number, customer_id, agent_code, SSN, VIN.
- Downstream queries/views perform point lookups on these columns.
- Table is a dimension or fact table (not staging).

### When NOT to recommend
- Columns used only in range queries (`BETWEEN`, `>=`) — clustering is better.
- Tables where all access is full-scan aggregation.
- Requires **Enterprise Edition or higher** — always note this prerequisite.

### DDL template
```sql
-- O3: Search optimization for point lookups on high-cardinality columns
-- Ref: https://docs.snowflake.com/en/user-guide/search-optimization-service
-- Requires: Enterprise Edition or higher
ALTER TABLE {schema}.{table_name}
  ADD SEARCH OPTIMIZATION ON EQUALITY({col1}, {col2});
```

### Detection logic
1. Identify columns with high-cardinality naming patterns (policy_number,
   claim_number, customer_id, etc.) in dimension and fact tables.
2. Cross-reference with downstream DML/views — look for `WHERE col = :param`
   or `WHERE col IN (...)` patterns.
3. Recommend specific columns, not blanket table-level search optimization.

### Severity rules
- High-cardinality column with confirmed point-lookup usage → **MEDIUM**
- High-cardinality column without confirmed downstream usage → **LOW**

### Classification
`manual-review` — requires Enterprise Edition; incurs additional storage and
compute cost for the search access path maintenance.

---

## O4: Query Acceleration Service

### When to recommend
- Warehouse executes ad-hoc analytic queries against fact or aggregate tables.
- Query patterns include large scans with selective filters that could benefit
  from offloading portions to shared compute resources.
- Workload is unpredictable (reporting, dashboards, data exploration).

### When NOT to recommend
- Batch ETL warehouses with predictable, optimized queries.
- Warehouses already sized appropriately for their workload.

### DDL template
```sql
-- O4: Query Acceleration for ad-hoc analytics workload
-- Ref: https://docs.snowflake.com/en/user-guide/query-acceleration-service
ALTER WAREHOUSE {warehouse_name}
  SET ENABLE_QUERY_ACCELERATION = TRUE
      QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
```

### Detection logic
1. If the converted workload includes reporting views or aggregate marts that
   suggest ad-hoc analytics usage, recommend QAS on the target warehouse.
2. This is always a general recommendation — specific warehouse names must be
   confirmed by the user.

### Severity rules
- Always **LOW** — QAS is a nice-to-have that depends on actual query patterns.

### Classification
`manual-review` — requires warehouse-level configuration decision and has
cost implications (billed per-query for accelerated portions).

---

## O5: Informational Constraints

### When to recommend
- **Always** recommend for all PK/FK relationships identified in the
  dependency_graph.json, regardless of table type.
- Snowflake does not enforce PK/FK constraints on standard tables, but they
  serve two important purposes:
  1. BI tools (Tableau, Power BI, Looker) use them for automatic join inference.
  2. The Snowflake query optimizer uses them for join elimination.

### When NOT to recommend
- Never skip — informational constraints are always beneficial and zero-cost.

### DDL template
```sql
-- O5: Informational PK constraint for BI tools and join elimination
-- Ref: https://docs.snowflake.com/en/sql-reference/constraints-overview
ALTER TABLE {schema}.{table_name}
  ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_columns});

-- O5: Informational FK constraint for BI tools and join elimination
ALTER TABLE {schema}.{child_table}
  ADD CONSTRAINT fk_{child_table}_{parent_table}
  FOREIGN KEY ({fk_columns}) REFERENCES {schema}.{parent_table} ({pk_columns});
```

### Detection logic
1. From dependency_graph.json, extract all parent→child relationships.
2. From object_inventory.json, identify primary key columns for each table.
3. From DML JOIN clauses, identify foreign key columns.
4. Generate PK constraints for all tables, FK constraints for all relationships.

### Severity rules
- Always **HIGH** — zero cost, always beneficial.

### Classification
`auto-apply` — informational only, not enforced, no runtime overhead.

---

## O6: Dynamic Table Candidates

### When to recommend
- Table is an **aggregate/mart** (prefix `agg_`, or located in aggregation DML).
- DML is a single `INSERT ... SELECT` (or `CREATE TABLE AS SELECT`) with no
  procedural logic (no IF/LOOP/CURSOR/MERGE).
- The SELECT references only tables that are already loaded (dimensions, facts).
- The aggregation would benefit from automatic refresh when source data changes.

### When NOT to recommend
- DML contains procedural logic, multi-step transformations, or MERGE statements.
- Table is part of a complex ETL chain where refresh ordering matters beyond
  what Dynamic Tables can express with `TARGET_LAG`.
- Table requires UPDATE/DELETE operations (Dynamic Tables are append-oriented).

### DDL template
```sql
-- O6: Dynamic Table candidate — declarative auto-refresh aggregation
-- Ref: https://docs.snowflake.com/en/user-guide/dynamic-tables-about
-- Replace existing CREATE TABLE + INSERT with:
CREATE OR REPLACE DYNAMIC TABLE {schema}.{table_name}
  TARGET_LAG = '{lag}'  -- e.g., '1 hour', 'DOWNSTREAM'
  WAREHOUSE = {warehouse_name}
AS
  {select_statement};
```

### Detection logic
1. Scan aggregation DML files for `INSERT INTO ... SELECT` patterns.
2. Verify the SELECT is a single statement with no procedural wrappers.
3. Extract the SELECT statement for the Dynamic Table definition.
4. Recommend `TARGET_LAG = '1 hour'` as default; note user should adjust
   based on freshness requirements.

### Severity rules
- Simple INSERT...SELECT aggregate with clear source tables → **MEDIUM**
- Complex SELECT with multiple CTEs but still single-statement → **LOW**

### Classification
`manual-review` — converting to Dynamic Table is an architectural decision that
changes the refresh model from batch-push to declarative-pull. Requires user
to confirm warehouse assignment and target lag.

---

## O7: Streams + Tasks Roadmap

### When to recommend
- The converted workload follows a batch ETL pattern:
  staging → dimensions → facts → aggregates.
- Source tables are loaded via COPY INTO or external stages.
- The dependency_graph shows a clear DAG of table dependencies.

### When NOT to recommend
- One-time migration loads (no ongoing refresh needed).
- User explicitly states batch ETL will remain the operational pattern.

### Output format
This rule produces a **roadmap document** (not executable DDL) describing:
1. Which staging tables could have Streams attached for CDC.
2. Which transformation steps could become Tasks triggered by Stream data.
3. The Task DAG structure mirroring the dependency_graph.
4. Estimated benefits (near-real-time refresh, reduced compute waste).

### Template
```sql
-- O7: Streams + Tasks modernization roadmap
-- Ref: https://docs.snowflake.com/en/user-guide/streams-intro
-- Ref: https://docs.snowflake.com/en/user-guide/tasks-intro
-- This is a ROADMAP — not auto-applied. Requires architectural decision.
--
-- Recommended Stream/Task pairs:
--   STREAM on {staging_table} → TASK to refresh {dim_table}
--   STREAM on {dim_table} → TASK to refresh {fact_table}
--   TASK DAG root: {root_task} → {child_task_1} → {child_task_2}
--
-- Benefits: Near-real-time refresh, reduced compute (process only changed rows),
-- built-in exactly-once semantics via Stream offsets.
```

### Severity rules
- Always **LOW** — this is advisory and architectural.

### Classification
`manual-review` — always. This is a fundamental architecture change.

---

## O8: Storage Tuning (DATA_RETENTION_TIME_IN_DAYS)

### When to recommend
- **Always** recommend for all tables, using table-type-based defaults:

| Table Type | Recommended Retention | Rationale |
|------------|----------------------|-----------|
| Staging (`stg_*`) | 0–1 day | Transitory data, reloaded each batch |
| Dimensions | 7–14 days | Operational recovery for slowly changing data |
| Facts | 30–90 days | Audit trail, point-in-time recovery |
| Audit/Log | 90 days | Compliance, investigation support |
| Aggregates | 7–14 days | Can be rebuilt from source tables |

### When NOT to recommend
- If the table is already TRANSIENT (Time Travel is limited to 0–1 day by design).
  In that case, note that retention is inherently limited.

### DDL template
```sql
-- O8: Storage tuning — set retention period based on table type
-- Ref: https://docs.snowflake.com/en/user-guide/data-time-travel
ALTER TABLE {schema}.{table_name}
  SET DATA_RETENTION_TIME_IN_DAYS = {days};
```

### Detection logic
1. Classify each table by type using naming conventions and layer assignment.
2. Apply the recommended retention from the table above.
3. Flag any tables where the default (1 day) may be insufficient.

### Severity rules
- Fact or audit table with default 1-day retention → **MEDIUM**
- Dimension table with default 1-day retention → **MEDIUM**
- Staging table (already minimal) → **LOW** (note only)

### Classification
`auto-apply` — changing retention is a metadata operation with no schema impact.
The only cost trade-off is increased Time Travel storage for longer retention.

---

## Severity Definitions

| Severity | Meaning | Action |
|----------|---------|--------|
| **HIGH** | Strong ROI, low risk, broadly applicable. Recommended for all migrations. | Apply unless explicitly excluded. |
| **MEDIUM** | Conditional on workload volume, query patterns, or edition. Beneficial but requires validation. | Present with context; apply on user confirmation. |
| **LOW** | Nice-to-have or architectural advisory. Benefits depend on operational decisions. | Present as informational; do not apply without explicit request. |

## Classification Definitions

| Classification | Meaning | Auto-apply Behavior |
|---------------|---------|-------------------|
| **auto-apply** | Safe, additive change. No schema alteration, no data impact, no edition requirement. | Can be applied to converted DDL files or emitted as separate DDL on user approval. |
| **manual-review** | Requires architecture decision, has cost implications, or needs Enterprise Edition. | Always presented as advisory. Never applied without per-item user approval. |

## Evaluation Order

Rules are evaluated in order O1→O8. A table may receive recommendations from
multiple rules (e.g., a fact table might get O1 + O5 + O8). All applicable
recommendations are included in the optimization_report.json.

## Cross-References

- **object_inventory.json** (Step 1) — table names, types, source files
- **dependency_graph.json** (Step 2) — parent→child relationships for O5 and O7
- **complexity_report.json** (Step 4) — row count indicators for O1 thresholds
- **Converted DDL files** (Step 5) — column definitions for O1 column selection
- **Converted DML files** (Step 5) — query patterns for O3 and O6 detection
- **ddl_views.sql** (Step 5) — downstream query patterns for O1 and O3
