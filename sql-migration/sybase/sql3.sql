-- ============================================================================= 

-- ETL SCRIPT 03: Load Fact Tables 

-- Description : Build fact tables by joining staged transactional data 

--               to dimension keys. Loads fact_policy, fact_claims, 

--               and fact_payments into the star schema. 

-- Target DB   : DataWarehouse (Sybase IQ) 

-- Author      : DW Team 

-- Created     : 2009-05-10 

-- Modified    : 2014-02-20 

-- ============================================================================= 

  

-- ============================================================ 

-- SECTION 1: FACT_POLICY  (grain = one row per policy period) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_policy' AND type = 'U') 

    DROP TABLE fact_policy 

GO 

  

CREATE TABLE fact_policy ( 

    policy_fact_key         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    -- Dimension keys 

    policy_id               BIGINT          NOT NULL,   -- degenerate dim 

    customer_key            BIGINT          NOT NULL, 

    agent_key               INT             NOT NULL, 

    product_key             INT             NOT NULL, 

    effective_date_key      INT             NOT NULL, 

    expiry_date_key         INT             NOT NULL, 

    -- Degenerate dimensions 

    policy_number           VARCHAR(30)     NOT NULL, 

    status_code             CHAR(2)         NOT NULL, 

    territory_code          VARCHAR(10)     NULL, 

    -- Measures 

    annual_premium          DECIMAL(18,2)   NULL, 

    coverage_amount         DECIMAL(18,2)   NULL, 

    policy_term_days        INT             NULL, 

    earned_premium_amount   DECIMAL(18,2)   NULL,   -- derived 

    -- Audit 

    dw_insert_ts            DATETIME        NOT NULL DEFAULT GETDATE(), 

    dw_batch_id             INT             NULL 

) 

GO 

  

-- Populate fact_policy by resolving surrogate keys 

-- Uses CURRENT SCD2 snapshot (scd_current_flag = 'Y') 

INSERT INTO fact_policy ( 

    policy_id, 

    customer_key, 

    agent_key, 

    product_key, 

    effective_date_key, 

    expiry_date_key, 

    policy_number, 

    status_code, 

    territory_code, 

    annual_premium, 

    coverage_amount, 

    policy_term_days, 

    earned_premium_amount, 

    dw_batch_id 

) 

SELECT 

    sp.policy_id, 

    -- Customer dimension key (current record) 

    ISNULL(dc.customer_key,  -1),           -- -1 = 'Unknown' member 

    -- Agent dimension key (current record) 

    ISNULL(da.agent_key,     -1), 

    -- Product dimension key 

    ISNULL(dp.product_key,   -1), 

    -- Date keys (YYYYMMDD integer) 

    CONVERT(INT, CONVERT(CHAR(8), sp.effective_date, 112)), 

    CONVERT(INT, CONVERT(CHAR(8), sp.expiry_date,    112)), 

    sp.policy_number, 

    sp.status_code, 

    sp.territory_code, 

    sp.annual_premium, 

    sp.coverage_amount, 

    DATEDIFF(DAY, sp.effective_date, sp.expiry_date), 

    -- Earned premium: prorate for non-full-year terms 

    ROUND( 

        sp.annual_premium * 

        (CAST(DATEDIFF(DAY, sp.effective_date, sp.expiry_date) AS DECIMAL(10,4)) / 365.0), 

        2 

    ) 

    ,1001   -- batch_id placeholder 

FROM stg_policies sp 

LEFT OUTER JOIN dim_customer dc 

    ON  dc.customer_id      = sp.customer_id 

    AND dc.scd_current_flag = 'Y' 

LEFT OUTER JOIN dim_agent da 

    ON  da.agent_id         = sp.agent_id 

    AND da.scd_current_flag = 'Y' 

LEFT OUTER JOIN dim_product dp 

    ON  dp.product_code     = sp.product_code 

-- Avoid duplicates: delete existing keys before reload 

WHERE NOT EXISTS ( 

    SELECT 1 FROM fact_policy fp 

    WHERE  fp.policy_id = sp.policy_id 

) 

GO 

  

-- Update existing fact rows where staging has newer data 

UPDATE fp 

SET    fp.status_code             = sp.status_code, 

       fp.annual_premium          = sp.annual_premium, 

       fp.coverage_amount         = sp.coverage_amount, 

       fp.earned_premium_amount   = ROUND( 

                                        sp.annual_premium * 

                                        (CAST(DATEDIFF(DAY, sp.effective_date, sp.expiry_date) 

                                         AS DECIMAL(10,4)) / 365.0), 2), 

       fp.dw_insert_ts            = GETDATE() 

FROM   fact_policy fp 

INNER JOIN stg_policies sp ON fp.policy_id = sp.policy_id 

WHERE  fp.status_code     <> sp.status_code 

    OR fp.annual_premium  <> ISNULL(sp.annual_premium, fp.annual_premium) 

GO 

  

-- ============================================================ 

-- SECTION 2: FACT_CLAIMS  (grain = one row per claim) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_claims' AND type = 'U') 

    DROP TABLE fact_claims 

GO 

  

CREATE TABLE fact_claims ( 

    claim_fact_key              BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    -- Dimension keys 

    claim_id                    BIGINT          NOT NULL,   -- degenerate 

    policy_id                   BIGINT          NOT NULL,   -- degenerate 

    customer_key                BIGINT          NOT NULL, 

    product_key                 INT             NOT NULL, 

    incident_date_key           INT             NOT NULL, 

    reported_date_key           INT             NOT NULL, 

    closed_date_key             INT             NULL, 

    -- Degenerate dimensions 

    claim_number                VARCHAR(30)     NOT NULL, 

    claim_type_code             VARCHAR(20)     NOT NULL, 

    status_code                 VARCHAR(10)     NOT NULL, 

    fault_indicator             CHAR(1)         NULL, 

    litigation_flag             CHAR(1)         NULL, 

    -- Measures 

    total_incurred              DECIMAL(18,2)   NULL, 

    total_paid                  DECIMAL(18,2)   NULL, 

    total_reserved              DECIMAL(18,2)   NULL, 

    outstanding_reserve         DECIMAL(18,2)   NULL,   -- derived 

    claim_report_lag_days       INT             NULL,   -- derived 

    claim_settlement_days       INT             NULL,   -- derived (null if open) 

    loss_ratio_pct              DECIMAL(10,4)   NULL,   -- derived vs policy premium 

    -- Audit 

    dw_insert_ts                DATETIME        NOT NULL DEFAULT GETDATE(), 

    dw_batch_id                 INT             NULL 

) 

GO 

  

INSERT INTO fact_claims ( 

    claim_id, 

    policy_id, 

    customer_key, 

    product_key, 

    incident_date_key, 

    reported_date_key, 

    closed_date_key, 

    claim_number, 

    claim_type_code, 

    status_code, 

    fault_indicator, 

    litigation_flag, 

    total_incurred, 

    total_paid, 

    total_reserved, 

    outstanding_reserve, 

    claim_report_lag_days, 

    claim_settlement_days, 

    loss_ratio_pct, 

    dw_batch_id 

) 

SELECT 

    sc.claim_id, 

    sc.policy_id, 

    ISNULL(dc.customer_key, -1), 

    ISNULL(dp.product_key,  -1), 

    CONVERT(INT, CONVERT(CHAR(8), sc.incident_date,  112)), 

    CONVERT(INT, CONVERT(CHAR(8), sc.reported_date,  112)), 

    CASE WHEN sc.closed_date IS NOT NULL 

         THEN CONVERT(INT, CONVERT(CHAR(8), sc.closed_date, 112)) 

         ELSE NULL END, 

    sc.claim_number, 

    sc.claim_type_code, 

    sc.status_code, 

    sc.fault_indicator, 

    sc.litigation_flag, 

    ISNULL(sc.total_incurred, 0), 

    ISNULL(sc.total_paid,     0), 

    ISNULL(sc.total_reserved, 0), 

    -- Outstanding = reserved minus paid 

    ISNULL(sc.total_reserved, 0) - ISNULL(sc.total_paid, 0), 

    -- Lag from incident to report (in days) 

    DATEDIFF(DAY, sc.incident_date, sc.reported_date), 

    -- Settlement days: null if still open 

    CASE WHEN sc.closed_date IS NOT NULL 

         THEN DATEDIFF(DAY, sc.reported_date, sc.closed_date) 

         ELSE NULL END, 

    -- Loss ratio against policy earned premium (requires join to fact_policy) 

    CASE WHEN ISNULL(fp.earned_premium_amount, 0) > 0 

         THEN ROUND(ISNULL(sc.total_incurred, 0) / fp.earned_premium_amount * 100.0, 4) 

         ELSE NULL END, 

    1001 

FROM stg_claims sc 

LEFT OUTER JOIN dim_customer dc 

    ON  dc.customer_id      = sc.customer_id 

    AND dc.scd_current_flag = 'Y' 

LEFT OUTER JOIN fact_policy fp 

    ON  fp.policy_id        = sc.policy_id 

LEFT OUTER JOIN dim_product dp 

    ON  dp.product_key      = fp.product_key 

WHERE NOT EXISTS ( 

    SELECT 1 FROM fact_claims fc WHERE fc.claim_id = sc.claim_id 

) 

GO 

  

-- Refresh measures for existing open claims (amounts change as payments made) 

UPDATE fc 

SET    fc.status_code           = sc.status_code, 

       fc.total_incurred        = ISNULL(sc.total_incurred, 0), 

       fc.total_paid            = ISNULL(sc.total_paid,     0), 

       fc.total_reserved        = ISNULL(sc.total_reserved, 0), 

       fc.outstanding_reserve   = ISNULL(sc.total_reserved, 0) - ISNULL(sc.total_paid, 0), 

       fc.closed_date_key       = CASE WHEN sc.closed_date IS NOT NULL 

                                       THEN CONVERT(INT, CONVERT(CHAR(8), sc.closed_date, 112)) 

                                       ELSE NULL END, 

       fc.claim_settlement_days = CASE WHEN sc.closed_date IS NOT NULL 

                                       THEN DATEDIFF(DAY, sc.reported_date, sc.closed_date) 

                                       ELSE NULL END, 

       fc.litigation_flag       = sc.litigation_flag, 

       fc.dw_insert_ts          = GETDATE() 

FROM   fact_claims fc 

INNER JOIN stg_claims sc ON fc.claim_id = sc.claim_id 

WHERE  fc.status_code       <> sc.status_code 

    OR fc.total_paid         < ISNULL(sc.total_paid,     0) 

    OR fc.total_reserved    <> ISNULL(sc.total_reserved, 0) 

GO 

  

-- ============================================================ 

-- SECTION 3: FACT_PAYMENTS  (grain = one row per payment txn) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_payments' AND type = 'U') 

    DROP TABLE fact_payments 

GO 

  

CREATE TABLE fact_payments ( 

    payment_fact_key            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    -- Dimension keys 

    payment_id                  BIGINT          NOT NULL,   -- degenerate 

    policy_id                   BIGINT          NOT NULL,   -- degenerate 

    customer_key                BIGINT          NOT NULL, 

    product_key                 INT             NOT NULL, 

    payment_date_key            INT             NOT NULL, 

    due_date_key                INT             NULL, 

    -- Degenerate dimensions 

    payment_method              VARCHAR(30)     NULL, 

    payment_status              VARCHAR(20)     NOT NULL, 

    installment_number          SMALLINT        NULL, 

    late_flag                   CHAR(1)         NULL, 

    nsf_flag                    CHAR(1)         NULL, 

    -- Measures 

    payment_amount              DECIMAL(18,2)   NOT NULL, 

    days_past_due               INT             NULL,   -- derived 

    -- Audit 

    dw_insert_ts                DATETIME        NOT NULL DEFAULT GETDATE(), 

    dw_batch_id                 INT             NULL 

) 

GO 

  

INSERT INTO fact_payments ( 

    payment_id, 

    policy_id, 

    customer_key, 

    product_key, 

    payment_date_key, 

    due_date_key, 

    payment_method, 

    payment_status, 

    installment_number, 

    late_flag, 

    nsf_flag, 

    payment_amount, 

    days_past_due, 

    dw_batch_id 

) 

SELECT 

    sp.payment_id, 

    sp.policy_id, 

    ISNULL(dc.customer_key, -1), 

    ISNULL(fp.product_key,  -1), 

    CONVERT(INT, CONVERT(CHAR(8), sp.payment_date, 112)), 

    CASE WHEN sp.due_date IS NOT NULL 

         THEN CONVERT(INT, CONVERT(CHAR(8), sp.due_date, 112)) 

         ELSE NULL END, 

    sp.payment_method, 

    sp.payment_status, 

    sp.installment_number, 

    sp.late_flag, 

    sp.nsf_flag, 

    sp.payment_amount, 

    CASE WHEN sp.due_date IS NOT NULL AND sp.payment_date > sp.due_date 

         THEN DATEDIFF(DAY, sp.due_date, sp.payment_date) 

         ELSE 0 END, 

    1001 

FROM stg_payments sp 

LEFT OUTER JOIN dim_customer dc 

    ON  dc.customer_id      = sp.customer_id 

    AND dc.scd_current_flag = 'Y' 

LEFT OUTER JOIN fact_policy fp 

    ON  fp.policy_id        = sp.policy_id 

WHERE NOT EXISTS ( 

    SELECT 1 FROM fact_payments pay WHERE pay.payment_id = sp.payment_id 

) 

GO 

  

-- ============================================================ 

-- SECTION 4: Post-load row count validation 

-- ============================================================ 

  

SELECT 'fact_policy'   AS fact_table, COUNT(*) AS total_rows, 

       SUM(annual_premium)  AS sum_premium 

FROM   fact_policy 

UNION ALL 

SELECT 'fact_claims',   COUNT(*), SUM(total_incurred) 

FROM   fact_claims 

UNION ALL 

SELECT 'fact_payments', COUNT(*), SUM(payment_amount) 

FROM   fact_payments 

GO 

  

-- Referential integrity check: unresolved dimension keys 

SELECT 'Unresolved customer keys' AS check_name, COUNT(*) AS cnt 

FROM   fact_policy WHERE customer_key = -1 

UNION ALL 

SELECT 'Unresolved agent keys',    COUNT(*) FROM fact_policy WHERE agent_key   = -1 

UNION ALL 

SELECT 'Unresolved product keys',  COUNT(*) FROM fact_policy WHERE product_key = -1 

UNION ALL 

SELECT 'Claims no customer key',   COUNT(*) FROM fact_claims WHERE customer_key = -1 

GO 

  

PRINT '== Fact table load complete ==' 

GO 

  
