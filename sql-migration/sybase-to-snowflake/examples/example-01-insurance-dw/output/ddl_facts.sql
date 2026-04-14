-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql3.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L3_fact (DDL)
-- [META] Objects:       fact_policy, fact_claims, fact_payments
-- =============================================================================

-- ============================================================
-- SECTION 1: FACT_POLICY (grain = one row per policy period)
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- ============================================================

CREATE OR REPLACE TABLE fact_policy (
    policy_fact_key         BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    dw_insert_ts            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_batch_id             INT             NULL
);

-- ============================================================
-- SECTION 2: FACT_CLAIMS (grain = one row per claim)
-- ============================================================

CREATE OR REPLACE TABLE fact_claims (
    claim_fact_key              BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_batch_id                 INT             NULL
);

-- ============================================================
-- SECTION 3: FACT_PAYMENTS (grain = one row per payment txn)
-- [CONVERT] SMALLINT → SMALLINT (native)
-- ============================================================

CREATE OR REPLACE TABLE fact_payments (
    payment_fact_key            BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_batch_id                 INT             NULL
);
