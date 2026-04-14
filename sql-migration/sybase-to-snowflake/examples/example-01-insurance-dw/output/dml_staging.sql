-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql1.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L1_staging (DML)
-- [META] Objects:       stg_policies, stg_customers, stg_claims,
--                       stg_agents, stg_products, stg_payments
-- =============================================================================
--
-- [NOTE] LINKED SERVER SOURCE PLACEHOLDERS (7 occurrences):
--   The original Sybase source used linked server notation (e.g., OperationalDB..ins_policy)
--   to reference tables in an external operational database. These have been converted to
--   SOURCE_DB.SOURCE_SCHEMA.<table> placeholder references marked with [TODO].
--   These are INTENTIONAL example placeholders — the actual source references are
--   environment-specific and must be mapped by the migration team to the appropriate
--   Snowflake source (e.g., external stage, shared database, replicated table, or
--   Snowpipe-ingested landing zone). Grep for "SOURCE_DB.SOURCE_SCHEMA" to find all 7.
--
-- -------------------------------------------------------
-- Extract Policies (incremental: last 90 days + all open)
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] DATEADD(DAY, -90, GETDATE()) → DATEADD(DAY, -90, CURRENT_TIMESTAMP())
-- [CONVERT] CONVERT(DATE, expr) → expr::DATE (Snowflake cast)
-- [REMOVE]  Linked server prefix DataWarehouse.. (target is local)
-- [TODO]    Source table OperationalDB..ins_policy must be mapped to
--           a Snowflake source (e.g., external stage, shared DB, or replicated table).
--           Replace SOURCE_DB.SOURCE_SCHEMA.ins_policy below with actual reference.
-- -------------------------------------------------------
INSERT INTO stg_policies
SELECT
    p.policy_id,
    p.policy_number,
    p.customer_id,
    p.agent_id,
    p.product_code,
    p.effective_date::DATE,                                     -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    p.expiry_date::DATE,                                        -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    p.status_code,
    p.annual_premium,
    p.coverage_amount,
    p.territory_code,
    p.underwriter_id,
    p.created_date,
    p.last_updated,
    CURRENT_TIMESTAMP()                                         -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.ins_policy p                       -- [TODO] Replace with actual Snowflake source
WHERE p.last_updated >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())  -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
   OR p.status_code IN ('AC', 'PE');                            -- Active or Pending always refreshed

-- -------------------------------------------------------
-- Extract Customers (changed in last 90 days)
-- [CONVERT] LTRIM/RTRIM preserved (Snowflake native)
-- [CONVERT] LOWER preserved (Snowflake native)
-- [CONVERT] LEFT preserved (Snowflake native)
-- [TODO]    Source table OperationalDB..customer_master
-- -------------------------------------------------------
INSERT INTO stg_customers
SELECT
    c.customer_id,
    LTRIM(RTRIM(c.first_name)),
    LTRIM(RTRIM(c.last_name)),
    c.dob::DATE,                                                -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    c.gender,
    c.marital_status,
    c.addr1,
    c.addr2,
    c.city,
    c.state,
    LEFT(c.zip, 10),
    LOWER(c.email_address),
    c.phone1,
    c.fico_score,
    c.segment_code,
    c.acquisition_channel,
    c.created_ts,
    c.modified_ts,
    CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.customer_master c                  -- [TODO] Replace with actual Snowflake source
WHERE c.modified_ts >= DATEADD(DAY, -90, CURRENT_TIMESTAMP());

-- -------------------------------------------------------
-- Extract Claims (open claims + recently modified)
-- [CONVERT] ISNULL() → COALESCE()
-- [TODO]    Source table OperationalDB..claims_header
-- -------------------------------------------------------
INSERT INTO stg_claims
SELECT
    cl.claim_id,
    cl.claim_no,
    cl.policy_id,
    cl.insured_customer_id,
    cl.loss_date::DATE,                                         -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    cl.report_date::DATE,
    cl.close_date::DATE,
    cl.claim_type,
    cl.claim_status,
    COALESCE(cl.total_incurred, 0.00),                          -- [CONVERT] ISNULL() → COALESCE()
    COALESCE(cl.paid_amount, 0.00),
    COALESCE(cl.reserve_amount, 0.00),
    cl.adjuster_id,
    cl.at_fault_ind,
    cl.litigation_ind,
    cl.created_ts,
    cl.modified_ts,
    CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.claims_header cl                   -- [TODO] Replace with actual Snowflake source
WHERE cl.claim_status NOT IN ('CL','WD')
   OR cl.modified_ts >= DATEADD(DAY, -90, CURRENT_TIMESTAMP());

-- -------------------------------------------------------
-- Extract Agents (full refresh - relatively small table)
-- [TODO]    Source tables OperationalDB..agent_master, agency_master
-- -------------------------------------------------------
INSERT INTO stg_agents
SELECT
    a.agent_id,
    a.agent_no,
    a.agent_full_name,
    a.agency_id,
    ag.agency_name,
    a.lic_state,
    a.lic_number,
    a.lic_expiry_dt::DATE,                                      -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    a.region_cd,
    a.distribution_channel,
    a.active_ind,
    a.hire_dt::DATE,
    a.term_dt::DATE,
    a.create_ts,
    a.update_ts,
    CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.agent_master a                     -- [TODO] Replace with actual Snowflake source
LEFT OUTER JOIN SOURCE_DB.SOURCE_SCHEMA.agency_master ag        -- [TODO] Replace with actual Snowflake source
    ON a.agency_id = ag.agency_id;

-- -------------------------------------------------------
-- Extract Products (full refresh - reference table)
-- [TODO]    Source table OperationalDB..product_master
-- -------------------------------------------------------
INSERT INTO stg_products
SELECT
    pr.product_cd,
    pr.product_desc,
    pr.product_line_cd,
    pr.lob_cd,
    pr.sub_lob_cd,
    pr.coverage_type_cd,
    pr.rating_plan_cd,
    pr.min_prem,
    pr.max_coverage_amt,
    pr.active_ind,
    pr.eff_dt::DATE,
    pr.disc_dt::DATE,
    pr.create_ts,
    pr.update_ts,
    CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.product_master pr;                 -- [TODO] Replace with actual Snowflake source

-- -------------------------------------------------------
-- Extract Payments (last 90 days)
-- [TODO]    Source table OperationalDB..payment_transactions
-- -------------------------------------------------------
INSERT INTO stg_payments
SELECT
    pmt.payment_id,
    pmt.policy_id,
    pmt.customer_id,
    pmt.payment_dt::DATE,                                       -- [CONVERT] CONVERT(DATE, ...) → ::DATE
    pmt.due_dt::DATE,
    pmt.payment_amt,
    pmt.payment_method_cd,
    pmt.payment_status_cd,
    pmt.invoice_no,
    pmt.installment_no,
    pmt.late_ind,
    pmt.nsf_ind,
    pmt.create_ts,
    pmt.update_ts,
    CURRENT_TIMESTAMP()
FROM SOURCE_DB.SOURCE_SCHEMA.payment_transactions pmt           -- [TODO] Replace with actual Snowflake source
WHERE pmt.payment_dt >= DATEADD(DAY, -90, CURRENT_TIMESTAMP());

-- -------------------------------------------------------
-- Basic row count validation
-- [REMOVE] GO batch separator removed
-- -------------------------------------------------------
SELECT 'stg_policies'  AS table_name, COUNT(*) AS row_count FROM stg_policies
UNION ALL
SELECT 'stg_customers',                COUNT(*)              FROM stg_customers
UNION ALL
SELECT 'stg_claims',                   COUNT(*)              FROM stg_claims
UNION ALL
SELECT 'stg_agents',                   COUNT(*)              FROM stg_agents
UNION ALL
SELECT 'stg_products',                 COUNT(*)              FROM stg_products
UNION ALL
SELECT 'stg_payments',                 COUNT(*)              FROM stg_payments;
