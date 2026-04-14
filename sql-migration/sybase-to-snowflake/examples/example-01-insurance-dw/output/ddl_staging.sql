-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql1.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L1_staging (DDL)
-- [META] Objects:       stg_policies, stg_customers, stg_claims,
--                       stg_agents, stg_products, stg_payments
-- =============================================================================

-- -------------------------------------------------------
-- Create Staging: Policies
-- [CONVERT] IF EXISTS/DROP/GO → CREATE OR REPLACE
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_policies (
    policy_id           BIGINT          NOT NULL,
    policy_number       VARCHAR(30)     NOT NULL,
    customer_id         BIGINT          NOT NULL,
    agent_id            INT             NOT NULL,
    product_code        VARCHAR(20)     NOT NULL,
    effective_date      DATE            NOT NULL,
    expiry_date         DATE            NOT NULL,
    status_code         CHAR(2)         NOT NULL,
    annual_premium      DECIMAL(18,2)   NULL,
    coverage_amount     DECIMAL(18,2)   NULL,
    territory_code      VARCHAR(10)     NULL,
    underwriter_id      INT             NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,       -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,       -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()  -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- Create Staging: Customers
-- [CONVERT] SMALLINT → SMALLINT (Snowflake native)
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_customers (
    customer_id         BIGINT          NOT NULL,
    first_name          VARCHAR(100)    NULL,
    last_name           VARCHAR(100)    NULL,
    date_of_birth       DATE            NULL,
    gender_code         CHAR(1)         NULL,
    marital_status      CHAR(1)         NULL,
    address_line1       VARCHAR(200)    NULL,
    address_line2       VARCHAR(200)    NULL,
    city                VARCHAR(100)    NULL,
    state_code          CHAR(2)         NULL,
    zip_code            VARCHAR(10)     NULL,
    email               VARCHAR(150)    NULL,
    phone_primary       VARCHAR(20)     NULL,
    credit_score        SMALLINT        NULL,
    customer_segment    VARCHAR(30)     NULL,
    acquisition_source  VARCHAR(50)     NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,
    last_updated        TIMESTAMP_NTZ   NOT NULL,
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- Create Staging: Claims
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_claims (
    claim_id            BIGINT          NOT NULL,
    claim_number        VARCHAR(30)     NOT NULL,
    policy_id           BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    incident_date       DATE            NOT NULL,
    reported_date       DATE            NOT NULL,
    closed_date         DATE            NULL,
    claim_type_code     VARCHAR(20)     NOT NULL,
    status_code         VARCHAR(10)     NOT NULL,
    total_incurred      DECIMAL(18,2)   NULL,
    total_paid          DECIMAL(18,2)   NULL,
    total_reserved      DECIMAL(18,2)   NULL,
    adjuster_id         INT             NULL,
    fault_indicator     CHAR(1)         NULL,
    litigation_flag     CHAR(1)         NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,
    last_updated        TIMESTAMP_NTZ   NOT NULL,
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- Create Staging: Agents
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_agents (
    agent_id            INT             NOT NULL,
    agent_number        VARCHAR(20)     NOT NULL,
    agent_name          VARCHAR(200)    NOT NULL,
    agency_id           INT             NULL,
    agency_name         VARCHAR(200)    NULL,
    license_state       CHAR(2)         NULL,
    license_number      VARCHAR(30)     NULL,
    license_expiry      DATE            NULL,
    region_code         VARCHAR(10)     NULL,
    channel_code        VARCHAR(20)     NULL,   -- INDEPENDENT, CAPTIVE, DIRECT
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    hire_date           DATE            NULL,
    termination_date    DATE            NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,
    last_updated        TIMESTAMP_NTZ   NOT NULL,
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- Create Staging: Products
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_products (
    product_code        VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(150)    NOT NULL,
    product_line        VARCHAR(50)     NULL,   -- AUTO, HOME, COMMERCIAL, LIFE
    lob_code            VARCHAR(20)     NULL,
    sub_lob_code        VARCHAR(20)     NULL,
    coverage_type       VARCHAR(50)     NULL,
    rating_plan         VARCHAR(30)     NULL,
    min_premium         DECIMAL(18,2)   NULL,
    max_coverage        DECIMAL(18,2)   NULL,
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    effective_date      DATE            NULL,
    discontinue_date    DATE            NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,
    last_updated        TIMESTAMP_NTZ   NOT NULL,
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- Create Staging: Payments
-- [CONVERT] SMALLINT → SMALLINT (Snowflake native)
-- -------------------------------------------------------
CREATE OR REPLACE TABLE stg_payments (
    payment_id          BIGINT          NOT NULL,
    policy_id           BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    payment_date        DATE            NOT NULL,
    due_date            DATE            NULL,
    payment_amount      DECIMAL(18,2)   NOT NULL,
    payment_method      VARCHAR(30)     NULL,   -- CHECK, ACH, CREDIT_CARD
    payment_status      VARCHAR(20)     NOT NULL,
    invoice_number      VARCHAR(30)     NULL,
    installment_number  SMALLINT        NULL,
    late_flag           CHAR(1)         NULL,
    nsf_flag            CHAR(1)         NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,
    last_updated        TIMESTAMP_NTZ   NOT NULL,
    src_extract_ts      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
