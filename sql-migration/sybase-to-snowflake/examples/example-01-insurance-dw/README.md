# Example 01 — P&C Insurance Data Warehouse ETL

## What this demonstrates

A complete Sybase ASE/IQ data warehouse ETL pipeline for a Property & Casualty (P&C)
insurance company. The pipeline covers all major conversion patterns: staging ingestion,
SCD Type 1 and Type 2 dimension loading, fact table population with surrogate key
resolution, data quality checks with audit logging, aggregation marts, and reporting views.

This is a real-world representative example spanning 5 SQL scripts and ~23 distinct
database objects.

## Input characteristics

- **5 SQL scripts** totaling ~70KB of Sybase SQL
- **Source system:** Sybase ASE (OperationalDB via linked server) → Sybase IQ (DataWarehouse)
- **Domain:** P&C Insurance — policies, claims, payments, agents, products, customers
- **Architecture:** Classic star schema with staging → dimension → fact → aggregation → view layers

### Script inventory

| File | Purpose | Key Objects | Key Patterns |
|------|---------|-------------|-------------|
| `sql1.sql` | Stage raw source data | 6 staging tables (stg_policies, stg_customers, stg_claims, stg_agents, stg_products, stg_payments) | Linked server `..` notation, `GETDATE()`, `CONVERT()`, `IF EXISTS/DROP/GO`, incremental extraction (90-day window) |
| `sql2.sql` | Load dimension tables | dim_date (SCD1, WHILE loop), dim_product (SCD1), dim_agent (SCD2), dim_customer (SCD2 with derived fields) | SCD Type 1 UPDATE+INSERT, SCD Type 2 expire+insert, `DATEPART()`, `DATENAME()`, `DATEDIFF()`, `IDENTITY()`, CASE expressions for derived banding |
| `sql3.sql` | Load fact tables | fact_policy, fact_claims, fact_payments | Surrogate key resolution via LEFT OUTER JOIN to SCD2 dims, `CONVERT(INT, CONVERT(CHAR(8), date, 112))` for date keys, derived measures (earned_premium, outstanding_reserve, loss_ratio_pct) |
| `sql4.sql` | Data quality & audit | etl_audit_log + 10 DQ checks | NULL key checks, duplicate detection, date logic validation, negative amounts, orphan records, row count vs prior run (5% threshold), procedural DECLARE/IF/ELSE blocks |
| `sql5.sql` | Aggregation & reporting | agg_policy_monthly, agg_claims_monthly, v_policy_detail, v_claims_analysis, v_exec_kpi_summary | GROUP BY aggregations, cross-aggregate UPDATE (loss ratio enrichment), complex multi-table JOINs, CASE-based KPI derivation |

## Notable conversion challenges

1. **dim_date WHILE loop:** Sybase uses a `WHILE @year <= 2030` loop to populate a date dimension row-by-row. Snowflake conversion should use a generator approach (`TABLE(GENERATOR(ROWCOUNT => n))`) or Snowflake Scripting WHILE loop.

2. **SCD Type 2 with derived fields (dim_customer):** The customer dimension includes derived banding columns (age_band, credit_band, region) computed via nested CASE expressions using `DATEPART` and `DATEDIFF`. These CASE expressions must be converted alongside the SCD2 expire/insert logic.

3. **Integer date keys via nested CONVERT:** The pattern `CONVERT(INT, CONVERT(CHAR(8), effective_date, 112))` generates integer date keys (e.g., 20250401). This must become `TO_NUMBER(TO_CHAR(effective_date, 'YYYYMMDD'))`.

4. **Linked server cross-database references:** `sql1.sql` references `OperationalDB..dbo.table_name`. These linked server references must be mapped to Snowflake fully qualified names or flagged for pre-loading configuration.

5. **Procedural DQ checks with batch variables:** `sql4.sql` uses `DECLARE @var` blocks with `IF/ELSE` branching to log pass/fail results per check. These must be wrapped in Snowflake Scripting blocks with proper variable scoping.

6. **Cross-aggregate UPDATE (loss ratio enrichment):** `sql5.sql` updates `agg_claims_monthly` by joining to `agg_policy_monthly` to compute loss ratios. This cross-table UPDATE syntax differs between Sybase and Snowflake.

## Expected complexity distribution

| Tier | Count | Objects |
|------|-------|---------|
| Simple | ~12 | 6 staging tables, 3 simple views, dim_date DDL, 2 aggregate DDLs |
| Medium | ~6 | dim_product (SCD1), 3 fact loads, agg_policy_monthly DML, agg_claims_monthly DML |
| Complex | ~5 | dim_agent (SCD2), dim_customer (SCD2 + derived), DQ audit procedure, exec KPI view, loss ratio enrichment |

## Expected fidelity scoring outcomes

Based on the complexity distribution and conversion patterns, the expected fidelity
scoring results for this example are:

| Object | Tier | Expected Fidelity | Likely Decision | Notes |
|--------|------|------------------|----------------|-------|
| 6 staging tables | Simple | 0.95+ | PASS | Direct translations with minimal Sybase constructs |
| dim_date | Simple | 0.90+ | PASS | WHILE loop conversion is straightforward |
| dim_product (SCD1) | Medium | 0.90+ | PASS | MERGE conversion well-documented in type-map |
| dim_agent (SCD2) | Complex | 0.80+ | PASS (reduced threshold) | SCD2 pattern is standard |
| dim_customer (SCD2) | Complex | 0.70-0.85 | PASS or RETRY | Derived CASE expressions may introduce scoring noise on edge cases |
| fact_policy | Medium | 0.90+ | PASS | Surrogate key resolution is pattern-based |
| fact_claims | Medium | 0.85+ | PASS | Standard fact load |
| fact_payments | Medium | 0.88+ | PASS | Standard fact load |
| DQ audit procedure | Complex | 0.60-0.75 | RETRY → QUARANTINE likely | Deep procedural branching with RAISERROR |
| agg_policy_monthly | Medium | 0.90+ | PASS | Aggregation is straightforward |
| agg_claims_monthly | Medium | 0.85+ | PASS | Cross-aggregate UPDATE needs care |
| v_policy_detail | Simple | 0.95+ | PASS | View translation |
| v_claims_analysis | Simple | 0.92+ | PASS | View translation |
| v_exec_kpi_summary | Medium | 0.88+ | PASS | Complex joins but standard SQL |

### Expected retry and quarantine patterns

- **Likely retry candidates:** `dim_customer` (derived age/credit banding may produce
  spot-check mismatches on edge case seed data with NULL DOB or future dates)
- **Likely quarantine candidate:** `dq_audit_procedure` (RAISERROR with custom severity,
  deep IF/ELSE branching, @@ROWCOUNT in conditional logic)
- **Expected outcome:** 20 PASS, 2 RETRY→PASS, 1 QUARANTINE

### Edge case seed data requirements

The fidelity scoring step should generate seed data with these edge cases per the
conversion-standards reference:

- NULL values in key columns (customer_id, policy_id) — tests NULL handling conversions
- Duplicate key rows — tests SCD2 expire logic
- Boundary dates: `1900-01-01`, `9999-12-31`, `2000-02-29` — tests date function conversions
- Empty strings in VARCHAR columns — tests ISNULL/COALESCE behavior difference
- Negative amounts in premium/payment columns — tests derived measure calculations
- Future dates beyond current date — tests age_band CASE expression

## Known gaps

- This example does not cover Sybase stored procedures with cursors or dynamic SQL
- No triggers or event-driven patterns — all batch ETL
- No Sybase IQ-specific constructs (e.g., `IQ UNIQUE`, `LOAD TABLE`) — focuses on standard SQL
- For cursor and dynamic SQL conversion examples, a future example should be added
