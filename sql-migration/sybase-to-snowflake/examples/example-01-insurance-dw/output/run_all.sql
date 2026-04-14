-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source:        examples/example-01-insurance-dw/input/ (sql1-sql5)
-- [META] Converted at:  2026-04-02
-- [META] Purpose:       Master execution script — runs all converted SQL in
--                       dependency order (topological sort from dependency_graph.json)
-- =============================================================================
--
-- EXECUTION ORDER (7 layers, 20 objects):
--   L1: Staging DDL + DML   (6 tables)
--   L2: Dimension DDL + DML (4 tables)
--   L3: Fact DDL + DML      (3 tables)
--   L4: Audit DDL + DQ SP   (1 table, 1 procedure)
--   L5: Aggregation DDL+DML (2 tables)
--   L6: Views               (3 views)
--
-- USAGE:
--   Execute each section in order. Sections are idempotent (CREATE OR REPLACE).
--   For the DQ procedure, call: CALL sp_etl_dq_checks(1001);
--
-- PREREQUISITES:
--   - Target schema must exist
--   - Source tables (OperationalDB refs) must be mapped in dml_staging.sql
--     (search for [TODO] placeholders)
-- =============================================================================

-- ===================== L1: STAGING =====================

-- DDL: Create 6 staging tables
-- File: ddl_staging.sql
-- Objects: stg_policies, stg_customers, stg_claims, stg_agents, stg_products, stg_payments

-- DML: Populate from source (requires [TODO] source mapping)
-- File: dml_staging.sql
-- Objects: INSERT INTO stg_* FROM SOURCE_DB.SOURCE_SCHEMA.*

-- ===================== L2: DIMENSIONS =====================

-- DDL: Create 4 dimension tables
-- File: ddl_dimensions.sql
-- Objects: dim_date, dim_product, dim_agent, dim_customer

-- DML: Populate dimensions (date generator, SCD1 MERGE, SCD2 expire+insert)
-- File: dml_dimensions.sql
-- Objects: dim_date (GENERATOR), dim_product (MERGE), dim_agent (SCD2), dim_customer (SCD2)

-- ===================== L3: FACTS =====================

-- DDL: Create 3 fact tables
-- File: ddl_facts.sql
-- Objects: fact_policy, fact_claims, fact_payments

-- DML: Populate facts (surrogate key resolution, derived measures)
-- File: dml_facts.sql
-- Objects: fact_policy (INSERT+UPDATE), fact_claims (INSERT+UPDATE), fact_payments (INSERT)

-- ===================== L4: AUDIT =====================

-- DDL: Create audit log table
-- File: ddl_audit.sql
-- Objects: etl_audit_log (CREATE IF NOT EXISTS)

-- PROCEDURAL: DQ checks stored procedure [QUARANTINE — requires manual review]
-- File: proc_etl_dq_checks.sql
-- Objects: sp_etl_dq_checks(P_BATCH_ID INT)
-- Execute: CALL sp_etl_dq_checks(1001);

-- ===================== L5: AGGREGATION =====================

-- DDL+DML: Create and populate aggregate marts
-- File: dml_aggregation.sql
-- Objects: agg_policy_monthly (DDL+INSERT), agg_claims_monthly (DDL+INSERT+UPDATE)

-- ===================== L6: VIEWS =====================

-- DDL: Create reporting views
-- File: ddl_views.sql
-- Objects: v_policy_detail, v_claims_analysis, v_exec_kpi_summary
