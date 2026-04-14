-- =============================================================================
-- MEDIUM-TIER CONVERTED OBJECTS (14 objects)
-- Converted from: Sybase ASE / IQ → Snowflake
-- Source files  : ETL_Script_01.sql, sql3.sql, sql5.sql
-- Converter     : Cortex Code (sql-migration skill)
-- Date          : 2026-04-02
-- =============================================================================
-- Type-map reference:
--   DATETIME       → TIMESTAMP_NTZ          [CONVERT]
--   TINYINT        → SMALLINT               [CONVERT]
--   MONEY          → NUMBER(19,4)           [CONVERT]
--   DECIMAL(n,m)   → NUMBER(n,m)            [CONVERT]
--   IDENTITY(1,1)  → AUTOINCREMENT          [CONVERT]
--   GETDATE()      → CURRENT_TIMESTAMP()    [CONVERT]
--   ISNULL(a,b)    → COALESCE(a,b)          [CONVERT]
--   IN DBASPACE    → (removed)              [REMOVE]
--   IQ UNIQUE(n)   → (removed)              [REMOVE]
--   sp_bindrule    → (removed / inlined)    [REMOVE]
--   sp_binddefault → (removed / inlined)    [REMOVE]
--   SET TEMPORARY OPTION → (removed)        [REMOVE]
--   GO             → (removed, use ;)       [REMOVE]
--   PRINT          → (removed)              [REMOVE]
--   CREATE RULE    → informational comment (CHECK not enforced in Snowflake)       [CONVERT]
--   CREATE DEFAULT → column DEFAULT         [CONVERT]
--   CONVERT(DATE,x,112) → TO_DATE(x,'YYYYMMDD')                     [CONVERT]
--   CONVERT(INT,CONVERT(CHAR(8),d,112)) → TO_NUMBER(TO_CHAR(d,'YYYYMMDD')) [CONVERT]
--   LTRIM(RTRIM(x))     → TRIM(x)                                   [CONVERT]
--   DATEDIFF(DAY,a,b)   → DATEDIFF('DAY',a,b)                       [CONVERT]
--   DATEADD(day,n,GETDATE()) → DATEADD('DAY',n,CURRENT_TIMESTAMP()) [CONVERT]
--   OperationalDB.dbo.table → [TODO] linked server ref               [CONVERT]
-- =============================================================================


-- #############################################################################
-- GROUP 1: Enhanced Staging DDL (9 tables from ETL_Script_01.sql)
-- #############################################################################


-- =============================================================================
-- OBJECT 1 of 14: stg_policies
-- Source : ETL_Script_01.sql lines 106-135
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          TINYINT → SMALLINT                                 [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_status_code' → informational comment (CHECK not enforced in Snowflake)  [CONVERT]
--          sp_bindrule 'rule_positive_amount' → CHECK         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          IQ UNIQUE(500000) → removed                        [REMOVE]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_policies (
    policy_id           BIGINT          NOT NULL,
    policy_number       VARCHAR(30)     NOT NULL,
    customer_id         BIGINT          NOT NULL,
    agent_id            INT             NOT NULL,
    product_code        VARCHAR(20)     NOT NULL,
    effective_date      DATE            NOT NULL,
    expiry_date         DATE            NOT NULL,
    status_code         CHAR(2)         NOT NULL,
    annual_premium      NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    coverage_amount     NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    territory_code      VARCHAR(10)     NULL,
    underwriter_id      INT             NULL,
    renewal_count       SMALLINT        NOT NULL DEFAULT 0,   -- [CONVERT] TINYINT → SMALLINT
    cancellation_reason VARCHAR(50)     NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_status_code' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_policies_status_code CHECK (status_code IN ('AC','IN','CN','PD','SU','EX','RN','LO'))

    -- [CONVERT] sp_bindrule 'rule_positive_amount' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_policies_annual_premium CHECK (annual_premium IS NULL OR annual_premium >= 0)
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule  'rule_status_code',    'stg_policies.status_code'    — inlined as CHECK
-- [REMOVE] sp_bindrule  'rule_positive_amount','stg_policies.annual_premium' — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_extract_ts',    'stg_policies.src_extract_ts' — inlined as DEFAULT
-- [REMOVE] ALTER TABLE stg_policies ADD CONSTRAINT uq_stg_policy_id IQ UNIQUE(500000) — IQ cardinality hint


-- =============================================================================
-- OBJECT 2 of 14: stg_customers
-- Source : ETL_Script_01.sql lines 137-169
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          MONEY → NUMBER(19,4)                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)      [CONVERT]
--          sp_bindrule 'rule_state_code' → informational comment (CHECK not enforced in Snowflake)   [CONVERT]
--          sp_binddefault 'def_active_flag' → DEFAULT         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

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
    phone_secondary     VARCHAR(20)     NULL,
    credit_score        SMALLINT        NULL,
    fico_band           VARCHAR(10)     NULL,
    customer_segment    VARCHAR(30)     NULL,
    acquisition_source  VARCHAR(50)     NULL,
    lifetime_value      NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    churn_risk_score    NUMBER(5,4)     NULL,       -- [CONVERT] DECIMAL(5,4) → NUMBER(5,4)
    is_vip              CHAR(1)         NOT NULL DEFAULT 'N',
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_customers_is_vip CHECK (is_vip IN ('Y', 'N'))

    -- [CONVERT] sp_bindrule 'rule_state_code' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_customers_state_code CHECK (state_code RLIKE '[A-Z][A-Z]')
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_customers.is_vip'           — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_state_code', 'stg_customers.state_code'       — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_active_flag', 'stg_customers.is_vip'           — inlined as DEFAULT 'N'
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_customers.src_extract_ts'   — inlined as DEFAULT


-- =============================================================================
-- OBJECT 3 of 14: stg_claims
-- Source : ETL_Script_01.sql lines 171-202
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s     [CONVERT]
--          sp_binddefault 'def_active_flag' → DEFAULT         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_claims (
    claim_id            BIGINT          NOT NULL,
    claim_number        VARCHAR(30)     NOT NULL,
    policy_id           BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    incident_date       DATE            NOT NULL,
    reported_date       DATE            NOT NULL,
    closed_date         DATE            NULL,
    claim_type_code     VARCHAR(20)     NOT NULL,
    sub_type_code       VARCHAR(20)     NULL,
    status_code         VARCHAR(10)     NOT NULL,
    total_incurred      NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_paid          NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_reserved      NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    salvage_amount      NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    subrogation_amount  NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    adjuster_id         INT             NULL,
    fault_indicator     CHAR(1)         NULL,
    litigation_flag     CHAR(1)         NULL,
    catastrophe_code    VARCHAR(10)     NULL,
    reinsurance_flag    CHAR(1)         NOT NULL DEFAULT 'N',
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_claims_litigation_flag CHECK (litigation_flag IN ('Y', 'N'))
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_claims_reinsurance_flag CHECK (reinsurance_flag IN ('Y', 'N'))
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_claims.litigation_flag'   — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_claims.reinsurance_flag'  — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_active_flag', 'stg_claims.reinsurance_flag'  — inlined as DEFAULT 'N'
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_claims.src_extract_ts'    — inlined as DEFAULT


-- =============================================================================
-- OBJECT 4 of 14: stg_agents
-- Source : ETL_Script_01.sql lines 204-232
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          TINYINT → SMALLINT                                 [CONVERT]
--          MONEY → NUMBER(19,4)                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)      [CONVERT]
--          sp_binddefault 'def_active_flag' → DEFAULT         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_agents (
    agent_id            INT             NOT NULL,
    agent_number        VARCHAR(20)     NOT NULL,
    agent_name          VARCHAR(200)    NOT NULL,
    agency_id           INT             NULL,
    agency_name         VARCHAR(200)    NULL,
    parent_agency_id    INT             NULL,       -- Self-referencing hierarchy
    hierarchy_level     SMALLINT        NULL,       -- [CONVERT] TINYINT → SMALLINT; 1=Direct, 2=Agency, 3=MGA
    license_state       CHAR(2)         NULL,
    license_number      VARCHAR(30)     NULL,
    license_expiry      DATE            NULL,
    region_code         VARCHAR(10)     NULL,
    district_code       VARCHAR(10)     NULL,
    channel_code        VARCHAR(20)     NULL,
    commission_tier     VARCHAR(10)     NULL,
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    hire_date           DATE            NULL,
    termination_date    DATE            NULL,
    ytd_premium_written NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_agents_active_flag CHECK (active_flag IN ('Y', 'N'))
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_agents.active_flag'       — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_active_flag', 'stg_agents.active_flag'       — inlined as DEFAULT 'Y'
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_agents.src_extract_ts'    — inlined as DEFAULT


-- =============================================================================
-- OBJECT 5 of 14: stg_products
-- Source : ETL_Script_01.sql lines 234-258
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          MONEY → NUMBER(19,4)                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s     [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_products (
    product_code        VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(200)    NOT NULL,
    product_line        VARCHAR(50)     NULL,
    lob_code            VARCHAR(10)     NULL,
    lob_description     VARCHAR(100)    NULL,
    coverage_type       VARCHAR(50)     NULL,
    base_rate           NUMBER(10,6)    NULL,       -- [CONVERT] DECIMAL(10,6) → NUMBER(10,6)
    min_premium         NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    max_coverage        NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    is_commercial       CHAR(1)         NOT NULL DEFAULT 'N',
    is_reinsurable      CHAR(1)         NOT NULL DEFAULT 'Y',
    filing_state        CHAR(2)         NULL,
    effective_date      DATE            NOT NULL,
    expiry_date         DATE            NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    last_updated        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_products_is_commercial CHECK (is_commercial IN ('Y', 'N'))
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_products_is_reinsurable CHECK (is_reinsurable IN ('Y', 'N'))
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_products.is_commercial'    — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_products.is_reinsurable'   — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_products.src_extract_ts'   — inlined as DEFAULT


-- =============================================================================
-- OBJECT 6 of 14: stg_payments
-- Source : ETL_Script_01.sql lines 260-283
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          MONEY → NUMBER(19,4)                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)      [CONVERT]
--          sp_bindrule 'rule_positive_amount' → CHECK         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_payments (
    payment_id          BIGINT          NOT NULL,
    payment_reference   VARCHAR(40)     NOT NULL,
    policy_id           BIGINT          NOT NULL,
    claim_id            BIGINT          NULL,
    payment_date        DATE            NOT NULL,
    payment_type_code   VARCHAR(20)     NOT NULL,
    payment_method      VARCHAR(20)     NULL,
    payment_amount      NUMBER(19,4)    NOT NULL,   -- [CONVERT] MONEY → NUMBER(19,4)
    applied_amount      NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    reversal_flag       CHAR(1)         NOT NULL DEFAULT 'N',
    reversal_date       DATE            NULL,
    reversal_reason     VARCHAR(200)    NULL,
    check_number        VARCHAR(30)     NULL,
    bank_routing        VARCHAR(9)      NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_payments_reversal_flag CHECK (reversal_flag IN ('Y', 'N'))

    -- [CONVERT] sp_bindrule 'rule_positive_amount' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_payments_payment_amount CHECK (payment_amount IS NULL OR payment_amount >= 0)
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',        'stg_payments.reversal_flag'    — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_positive_amount','stg_payments.payment_amount'   — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_extract_ts',      'stg_payments.src_extract_ts'   — inlined as DEFAULT


-- =============================================================================
-- OBJECT 7 of 14: stg_territories
-- Source : ETL_Script_01.sql lines 286-308
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_state_code' → informational comment (CHECK not enforced in Snowflake)   [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s     [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_territories (
    territory_code      VARCHAR(10)     NOT NULL,
    territory_name      VARCHAR(100)    NOT NULL,
    state_code          CHAR(2)         NOT NULL,
    region_code         VARCHAR(10)     NOT NULL,
    region_name         VARCHAR(100)    NULL,
    zone_code           VARCHAR(10)     NULL,
    zone_name           VARCHAR(100)    NULL,
    catastrophe_zone    VARCHAR(10)     NULL,
    wind_pool_flag      CHAR(1)         NOT NULL DEFAULT 'N',
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    effective_date      DATE            NOT NULL,
    expiry_date         DATE            NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),  -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_state_code' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_territories_state_code CHECK (state_code RLIKE '[A-Z][A-Z]')

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)s
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_territories_wind_pool_flag CHECK (wind_pool_flag IN ('Y', 'N'))
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_territories_active_flag CHECK (active_flag IN ('Y', 'N'))
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_state_code', 'stg_territories.state_code'       — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_territories.wind_pool_flag'   — inlined as CHECK
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_territories.active_flag'      — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_territories.src_extract_ts'   — inlined as DEFAULT


-- =============================================================================
-- OBJECT 8 of 14: stg_reinsurance
-- Source : ETL_Script_01.sql lines 311-333
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          TINYINT → SMALLINT                                 [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_reinsurance (
    cession_id          BIGINT          NOT NULL,
    treaty_code         VARCHAR(20)     NOT NULL,
    treaty_type         VARCHAR(20)     NULL,       -- QS, XL, FAC
    policy_id           BIGINT          NOT NULL,
    claim_id            BIGINT          NULL,
    cedant_amount       NUMBER(18,2)    NOT NULL,   -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    reinsurer_share_pct NUMBER(7,4)     NOT NULL,   -- [CONVERT] DECIMAL(7,4) → NUMBER(7,4)
    reinsurer_amount    NUMBER(18,2)    NOT NULL,   -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    reinsurer_code      VARCHAR(20)     NOT NULL,
    layer_number        SMALLINT        NOT NULL DEFAULT 1,   -- [CONVERT] TINYINT → SMALLINT
    retention_amount    NUMBER(18,2)    NULL,       -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    reinstatement_flag  CHAR(1)         NOT NULL DEFAULT 'N',
    settlement_date     DATE            NULL,
    period_start        DATE            NOT NULL,
    period_end          DATE            NOT NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP()   -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_binddefault 'def_extract_ts', 'stg_reinsurance.src_extract_ts' — inlined as DEFAULT


-- =============================================================================
-- OBJECT 9 of 14: stg_underwriters
-- Source : ETL_Script_01.sql lines 336-356
-- Type   : TABLE (staging)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IN DBASPACE → removed                              [REMOVE]
--          MONEY → NUMBER(19,4)                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)      [CONVERT]
--          sp_binddefault 'def_active_flag' → DEFAULT         [CONVERT]
--          sp_binddefault 'def_extract_ts' → DEFAULT          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE stg_underwriters (
    underwriter_id      INT             NOT NULL,
    underwriter_code    VARCHAR(20)     NOT NULL,
    underwriter_name    VARCHAR(200)    NOT NULL,
    department_code     VARCHAR(20)     NULL,
    authority_level     VARCHAR(10)     NULL,       -- JR, SR, EXEC
    max_line_amount     NUMBER(19,4)    NULL,       -- [CONVERT] MONEY → NUMBER(19,4)
    lob_speciality      VARCHAR(50)     NULL,
    license_state       CHAR(2)         NULL,
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    start_date          DATE            NOT NULL,
    end_date            DATE            NULL,
    created_date        TIMESTAMP_NTZ   NOT NULL,   -- [CONVERT] DATETIME → TIMESTAMP_NTZ
    src_extract_ts      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP()   -- [CONVERT] DATETIME → TIMESTAMP_NTZ; sp_binddefault 'def_extract_ts' → DEFAULT CURRENT_TIMESTAMP()

    -- [CONVERT] sp_bindrule 'rule_yn_flag' → informational comment (CHECK not enforced in Snowflake)
    -- [TODO] Informational constraint (Snowflake does not enforce CHECK): CONSTRAINT chk_stg_underwriters_active_flag CHECK (active_flag IN ('Y', 'N'))
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent
-- [REMOVE] sp_bindrule    'rule_yn_flag',    'stg_underwriters.active_flag'     — inlined as CHECK
-- [REMOVE] sp_binddefault 'def_active_flag', 'stg_underwriters.active_flag'     — inlined as DEFAULT 'Y'
-- [REMOVE] sp_binddefault 'def_extract_ts',  'stg_underwriters.src_extract_ts'  — inlined as DEFAULT


-- #############################################################################
-- GROUP 2: Enhanced Staging DML (ETL_Script_01.sql lines 366-560)
-- INSERT...SELECT statements loading from OperationalDB linked server
-- #############################################################################


-- =============================================================================
-- DML 1 of 6: INSERT INTO stg_policies
-- Source : ETL_Script_01.sql lines 366-392
-- Changes: OperationalDB.dbo.ins_policy → [TODO] replace     [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                         [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))           [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')   [CONVERT]
--          ISNULL(x, 0) → COALESCE(x, 0)                     [CONVERT]
--          NULLIF(LTRIM(RTRIM(x)),'') → NULLIF(TRIM(x),'')  [CONVERT]
--          DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP()) [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

-- Incremental policy extract: last 90 days + all active
INSERT INTO stg_policies (
    policy_id, policy_number, customer_id, agent_id, product_code,
    effective_date, expiry_date, status_code, annual_premium, coverage_amount,
    territory_code, underwriter_id, renewal_count, cancellation_reason,
    created_date, last_updated
)
SELECT
    p.ins_policy_id,
    TRIM(p.policy_no),                                     -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    p.cust_id,
    p.agent_id,
    UPPER(TRIM(p.product_cd)),                             -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TO_DATE(p.eff_dt, 'YYYYMMDD'),                         -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    TO_DATE(p.exp_dt, 'YYYYMMDD'),                         -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    UPPER(TRIM(p.status_cd)),                              -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    p.annual_prem,
    p.coverage_amt,
    TRIM(p.terr_cd),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    p.underwriter_id,
    COALESCE(p.renewal_cnt, 0),                            -- [CONVERT] ISNULL(x, 0) → COALESCE(x, 0)
    NULLIF(TRIM(p.cancel_rsn), ''),                        -- [CONVERT] NULLIF(LTRIM(RTRIM(x)),'') → NULLIF(TRIM(x),'')
    p.create_dt,
    p.upd_dt
FROM ins_policy p                                          -- [TODO] Replace OperationalDB.dbo.ins_policy with Snowflake source
WHERE p.upd_dt >= DATEADD('DAY', -90, CURRENT_TIMESTAMP()) -- [CONVERT] DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP())
   OR p.status_cd IN ('AC', 'RN')                          -- All active/renewal regardless of date
;


-- =============================================================================
-- DML 2 of 6: TRUNCATE + INSERT INTO stg_agents
-- Source : ETL_Script_01.sql lines 394-433
-- Changes: OperationalDB.dbo.agent_master → [TODO] replace   [CONVERT]
--          OperationalDB.dbo.agency_master → [TODO] replace   [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                         [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))           [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')   [CONVERT]
--          ISNULL(x, 'STD') → COALESCE(x, 'STD')             [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                    [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

-- Full-refresh agents (small table)
TRUNCATE TABLE stg_agents;

INSERT INTO stg_agents (
    agent_id, agent_number, agent_name, agency_id, agency_name,
    parent_agency_id, hierarchy_level, license_state, license_number, license_expiry,
    region_code, district_code, channel_code, commission_tier,
    active_flag, hire_date, termination_date, ytd_premium_written,
    created_date, last_updated
)
SELECT
    a.agent_id,
    TRIM(a.agent_no),                                      -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TRIM(a.agent_nm),                                      -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    a.agency_id,
    TRIM(ag.agency_nm),                                    -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    ag.parent_agency_id,
    CASE
        WHEN ag.parent_agency_id IS NULL THEN 1
        WHEN ag2.parent_agency_id IS NULL THEN 2
        ELSE 3
    END,
    UPPER(TRIM(a.lic_state)),                              -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(a.lic_no),                                        -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TO_DATE(a.lic_exp_dt, 'YYYYMMDD'),                     -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    TRIM(a.region_cd),                                     -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TRIM(a.district_cd),                                   -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(a.channel_cd)),                             -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    COALESCE(TRIM(a.comm_tier), 'STD'),                    -- [CONVERT] ISNULL(LTRIM(RTRIM(x)),'STD') → COALESCE(TRIM(x),'STD')
    CASE WHEN a.term_dt IS NULL OR a.term_dt > CURRENT_TIMESTAMP() THEN 'Y' ELSE 'N' END, -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
    TO_DATE(a.hire_dt, 'YYYYMMDD'),                        -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    CASE WHEN a.term_dt IS NOT NULL THEN TO_DATE(a.term_dt, 'YYYYMMDD') ELSE NULL END, -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    a.ytd_prem,
    a.create_dt,
    a.upd_dt
FROM agent_master a                                        -- [TODO] Replace OperationalDB.dbo.agent_master with Snowflake source
LEFT JOIN agency_master ag ON ag.agency_id = a.agency_id   -- [TODO] Replace OperationalDB.dbo.agency_master with Snowflake source
LEFT JOIN agency_master ag2 ON ag2.agency_id = ag.parent_agency_id -- [TODO] Replace OperationalDB.dbo.agency_master with Snowflake source
;


-- =============================================================================
-- DML 3 of 6: INSERT INTO stg_customers
-- Source : ETL_Script_01.sql lines 436-476
-- Changes: OperationalDB.dbo.customer_master → [TODO] replace [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                         [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))           [CONVERT]
--          LOWER(LTRIM(RTRIM(x))) → LOWER(TRIM(x))           [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')   [CONVERT]
--          ISNULL(x, 'STANDARD') → COALESCE(x, 'STANDARD')   [CONVERT]
--          NULLIF(LTRIM(RTRIM(x)),'') → NULLIF(TRIM(x),'')  [CONVERT]
--          DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP()) [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

-- Incremental customer extract
INSERT INTO stg_customers (
    customer_id, first_name, last_name, date_of_birth, gender_code,
    marital_status, address_line1, address_line2, city, state_code, zip_code,
    email, phone_primary, phone_secondary, credit_score, fico_band,
    customer_segment, acquisition_source, lifetime_value, churn_risk_score, is_vip,
    created_date, last_updated
)
SELECT
    c.cust_id,
    TRIM(c.first_nm),                                      -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TRIM(c.last_nm),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TO_DATE(c.dob, 'YYYYMMDD'),                            -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    UPPER(TRIM(c.gender_cd)),                              -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    UPPER(TRIM(c.marital_cd)),                             -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(c.addr1),                                         -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    NULLIF(TRIM(c.addr2), ''),                             -- [CONVERT] NULLIF(LTRIM(RTRIM(x)),'') → NULLIF(TRIM(x),'')
    TRIM(c.city_nm),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(c.state_cd)),                               -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(c.zip),                                           -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    LOWER(TRIM(c.email_addr)),                             -- [CONVERT] LOWER(LTRIM(RTRIM(x))) → LOWER(TRIM(x))
    TRIM(c.phone1),                                        -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    NULLIF(TRIM(c.phone2), ''),                            -- [CONVERT] NULLIF(LTRIM(RTRIM(x)),'') → NULLIF(TRIM(x),'')
    c.credit_score,
    CASE
        WHEN c.credit_score >= 800 THEN 'EXCEPTIONAL'
        WHEN c.credit_score >= 740 THEN 'VERY_GOOD'
        WHEN c.credit_score >= 670 THEN 'GOOD'
        WHEN c.credit_score >= 580 THEN 'FAIR'
        WHEN c.credit_score IS NOT NULL THEN 'POOR'
        ELSE 'UNKNOWN'
    END,
    COALESCE(TRIM(c.segment_cd), 'STANDARD'),              -- [CONVERT] ISNULL(LTRIM(RTRIM(x)),'STANDARD') → COALESCE(TRIM(x),'STANDARD')
    TRIM(c.acq_source),                                    -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    c.ltv_amount,
    c.churn_score,
    CASE WHEN c.ltv_amount > 50000 THEN 'Y' ELSE 'N' END,
    c.create_dt,
    c.upd_dt
FROM customer_master c                                     -- [TODO] Replace OperationalDB.dbo.customer_master with Snowflake source
WHERE c.upd_dt >= DATEADD('DAY', -90, CURRENT_TIMESTAMP()) -- [CONVERT] DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP())
;


-- =============================================================================
-- DML 4 of 6: TRUNCATE + INSERT INTO stg_territories
-- Source : ETL_Script_01.sql lines 478-504
-- Changes: OperationalDB.dbo.territory_master → [TODO] replace [CONVERT]
--          OperationalDB.dbo.region_ref → [TODO] replace       [CONVERT]
--          OperationalDB.dbo.zone_ref → [TODO] replace         [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                          [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))            [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')    [CONVERT]
--          ISNULL(x, 'N') → COALESCE(x, 'N')                  [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                     [CONVERT]
--          GO → removed                                        [REMOVE]
-- =============================================================================

-- Full-refresh territory (small reference table)
TRUNCATE TABLE stg_territories;

INSERT INTO stg_territories (
    territory_code, territory_name, state_code, region_code, region_name,
    zone_code, zone_name, catastrophe_zone, wind_pool_flag, active_flag,
    effective_date, expiry_date, created_date
)
SELECT
    UPPER(TRIM(t.terr_cd)),                                -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(t.terr_nm),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(t.state_cd)),                               -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    UPPER(TRIM(t.region_cd)),                              -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(r.region_nm),                                     -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(t.zone_cd)),                                -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(z.zone_nm),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TRIM(t.cat_zone),                                      -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    COALESCE(t.wind_pool_flag, 'N'),                       -- [CONVERT] ISNULL(x, 'N') → COALESCE(x, 'N')
    CASE WHEN t.exp_dt IS NULL OR t.exp_dt > CURRENT_TIMESTAMP() THEN 'Y' ELSE 'N' END, -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
    TO_DATE(t.eff_dt, 'YYYYMMDD'),                         -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    CASE WHEN t.exp_dt IS NOT NULL THEN TO_DATE(t.exp_dt, 'YYYYMMDD') ELSE NULL END, -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    t.create_dt
FROM territory_master t                                    -- [TODO] Replace OperationalDB.dbo.territory_master with Snowflake source
LEFT JOIN region_ref r ON r.region_cd = t.region_cd        -- [TODO] Replace OperationalDB.dbo.region_ref with Snowflake source
LEFT JOIN zone_ref z ON z.zone_cd = t.zone_cd              -- [TODO] Replace OperationalDB.dbo.zone_ref with Snowflake source
;


-- =============================================================================
-- DML 5 of 6: INSERT INTO stg_reinsurance
-- Source : ETL_Script_01.sql lines 507-532
-- Changes: OperationalDB.dbo.reinsurance_cessions → [TODO] replace [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                              [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))                [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')        [CONVERT]
--          ISNULL(x, 1) → COALESCE(x, 1)                          [CONVERT]
--          ISNULL(x, 'N') → COALESCE(x, 'N')                      [CONVERT]
--          DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP()) [CONVERT]
--          GO → removed                                            [REMOVE]
-- =============================================================================

-- Incremental reinsurance cessions
INSERT INTO stg_reinsurance (
    cession_id, treaty_code, treaty_type, policy_id, claim_id,
    cedant_amount, reinsurer_share_pct, reinsurer_amount,
    reinsurer_code, layer_number, retention_amount, reinstatement_flag,
    settlement_date, period_start, period_end, created_date
)
SELECT
    r.cession_id,
    TRIM(r.treaty_cd),                                     -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(r.treaty_type)),                            -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    r.policy_id,
    r.claim_id,
    r.cedant_amt,
    r.reins_share_pct / 100.0,                             -- Source stores as percentage integer
    r.reins_amt,
    UPPER(TRIM(r.reins_cd)),                               -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    COALESCE(r.layer_no, 1),                               -- [CONVERT] ISNULL(x, 1) → COALESCE(x, 1)
    r.retention_amt,
    COALESCE(r.reinstatement_flag, 'N'),                   -- [CONVERT] ISNULL(x, 'N') → COALESCE(x, 'N')
    CASE WHEN r.settle_dt IS NOT NULL THEN TO_DATE(r.settle_dt, 'YYYYMMDD') ELSE NULL END, -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    TO_DATE(r.period_start, 'YYYYMMDD'),                   -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    TO_DATE(r.period_end, 'YYYYMMDD'),                     -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    r.create_dt
FROM reinsurance_cessions r                                -- [TODO] Replace OperationalDB.dbo.reinsurance_cessions with Snowflake source
WHERE r.create_dt >= DATEADD('DAY', -90, CURRENT_TIMESTAMP()) -- [CONVERT] DATEADD(day,-90,GETDATE()) → DATEADD('DAY',-90,CURRENT_TIMESTAMP())
;


-- =============================================================================
-- DML 6 of 6: TRUNCATE + INSERT INTO stg_underwriters
-- Source : ETL_Script_01.sql lines 534-557
-- Changes: OperationalDB.dbo.underwriter_master → [TODO] replace [CONVERT]
--          LTRIM(RTRIM(x)) → TRIM(x)                            [CONVERT]
--          UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))              [CONVERT]
--          CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')      [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                       [CONVERT]
--          GO → removed                                          [REMOVE]
--          PRINT → removed                                       [REMOVE]
-- =============================================================================

-- Full-refresh underwriters
TRUNCATE TABLE stg_underwriters;

INSERT INTO stg_underwriters (
    underwriter_id, underwriter_code, underwriter_name, department_code,
    authority_level, max_line_amount, lob_speciality, license_state,
    active_flag, start_date, end_date, created_date
)
SELECT
    u.uw_id,
    UPPER(TRIM(u.uw_cd)),                                  -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    TRIM(u.uw_nm),                                         -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    TRIM(u.dept_cd),                                       -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(u.auth_level)),                             -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    u.max_line_amt,
    TRIM(u.lob_spec),                                      -- [CONVERT] LTRIM(RTRIM(x)) → TRIM(x)
    UPPER(TRIM(u.lic_state)),                              -- [CONVERT] UPPER(LTRIM(RTRIM(x))) → UPPER(TRIM(x))
    CASE WHEN u.end_dt IS NULL OR u.end_dt > CURRENT_TIMESTAMP() THEN 'Y' ELSE 'N' END, -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
    TO_DATE(u.start_dt, 'YYYYMMDD'),                       -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    CASE WHEN u.end_dt IS NOT NULL THEN TO_DATE(u.end_dt, 'YYYYMMDD') ELSE NULL END, -- [CONVERT] CONVERT(DATE, x, 112) → TO_DATE(x, 'YYYYMMDD')
    u.create_dt
FROM underwriter_master u                                  -- [TODO] Replace OperationalDB.dbo.underwriter_master with Snowflake source
;
-- [REMOVE] PRINT 'Script 01 Complete: ...' — Sybase PRINT statement; no Snowflake equivalent


-- #############################################################################
-- GROUP 3: Basic Aggregate Tables (sql5.sql)
-- #############################################################################


-- =============================================================================
-- OBJECT 10 of 14: agg_policy_monthly (DDL + INSERT)
-- Source : sql5.sql lines 31-185
-- Type   : TABLE (aggregate mart) + INSERT
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IDENTITY(1,1) → AUTOINCREMENT                      [CONVERT]
--          TINYINT → SMALLINT                                 [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                    [CONVERT]
--          ISNULL(x, default) → COALESCE(x, default)          [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE agg_policy_monthly (
    agg_key                 INT AUTOINCREMENT NOT NULL PRIMARY KEY, -- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
    -- Grain dimensions
    year_number             SMALLINT        NOT NULL,
    month_number            SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    quarter_number          SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    product_line            VARCHAR(50)     NOT NULL,
    lob_code                VARCHAR(20)     NOT NULL,
    channel_code            VARCHAR(20)     NOT NULL,
    region                  VARCHAR(30)     NOT NULL,
    state_code              CHAR(2)         NOT NULL,
    status_code             CHAR(2)         NOT NULL,
    -- Policy measures
    policy_count            INT             NOT NULL DEFAULT 0,
    new_policy_count        INT             NOT NULL DEFAULT 0,
    cancelled_policy_count  INT             NOT NULL DEFAULT 0,
    total_annual_premium    NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_earned_premium    NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_coverage_amount   NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    avg_premium_per_policy  NUMBER(18,2)    NULL,                  -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    -- Renewal / retention metrics
    renewed_policy_count    INT             NOT NULL DEFAULT 0,
    -- Audit
    dw_insert_ts            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() -- [CONVERT] DATETIME DEFAULT GETDATE() → TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate monthly policy aggregates
INSERT INTO agg_policy_monthly (
    year_number, month_number, quarter_number,
    product_line, lob_code, channel_code, region, state_code, status_code,
    policy_count, new_policy_count, cancelled_policy_count,
    total_annual_premium, total_earned_premium, total_coverage_amount,
    avg_premium_per_policy
)
SELECT
    dd_eff.year_number,
    dd_eff.month_number,
    dd_eff.quarter_number,
    COALESCE(dp.product_line, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dp.lob_code, 'UNKNOWN'),                      -- [CONVERT] ISNULL → COALESCE
    COALESCE(da.channel_code, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dc.region, 'UNKNOWN'),                        -- [CONVERT] ISNULL → COALESCE
    COALESCE(dc.state_code, 'XX'),                         -- [CONVERT] ISNULL → COALESCE
    fp.status_code,
    COUNT(fp.policy_fact_key)                   AS policy_count,
    SUM(CASE WHEN dd_eff.year_number  = dd_eff.year_number
              AND dd_eff.month_number = dd_eff.month_number
             THEN 1 ELSE 0 END)                 AS new_policy_count,    -- simplified
    SUM(CASE WHEN fp.status_code = 'CN' THEN 1 ELSE 0 END) AS cancelled_count,
    SUM(COALESCE(fp.annual_premium, 0))         AS total_annual_premium, -- [CONVERT] ISNULL → COALESCE
    SUM(COALESCE(fp.earned_premium_amount, 0))  AS total_earned_premium, -- [CONVERT] ISNULL → COALESCE
    SUM(COALESCE(fp.coverage_amount, 0))        AS total_coverage_amount, -- [CONVERT] ISNULL → COALESCE
    CASE WHEN COUNT(fp.policy_fact_key) > 0
         THEN ROUND(SUM(COALESCE(fp.annual_premium, 0))
                     / COUNT(fp.policy_fact_key), 2)
         ELSE NULL END
FROM fact_policy fp
INNER JOIN dim_date    dd_eff ON dd_eff.date_key     = fp.effective_date_key
INNER JOIN dim_product dp     ON dp.product_key      = fp.product_key
INNER JOIN dim_agent   da     ON da.agent_key        = fp.agent_key
INNER JOIN dim_customer dc    ON dc.customer_key     = fp.customer_key
WHERE dd_eff.year_number >= 2015   -- rolling window; adjust as needed
GROUP BY
    dd_eff.year_number,
    dd_eff.month_number,
    dd_eff.quarter_number,
    COALESCE(dp.product_line, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dp.lob_code, 'UNKNOWN'),                      -- [CONVERT] ISNULL → COALESCE
    COALESCE(da.channel_code, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dc.region, 'UNKNOWN'),                        -- [CONVERT] ISNULL → COALESCE
    COALESCE(dc.state_code, 'XX'),                         -- [CONVERT] ISNULL → COALESCE
    fp.status_code
;


-- =============================================================================
-- OBJECT 11 of 14: agg_claims_monthly (DDL + INSERT + UPDATE)
-- Source : sql5.sql lines 197-387
-- Type   : TABLE (aggregate mart) + INSERT + UPDATE
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IDENTITY(1,1) → AUTOINCREMENT                      [CONVERT]
--          TINYINT → SMALLINT                                 [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                    [CONVERT]
--          ISNULL(x, default) → COALESCE(x, default)          [CONVERT]
--          CAST(x AS DECIMAL) → CAST(x AS NUMBER)             [CONVERT]
--          Sybase UPDATE...FROM → Snowflake UPDATE...FROM     [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE agg_claims_monthly (
    agg_key                     INT AUTOINCREMENT NOT NULL PRIMARY KEY, -- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
    year_number                 SMALLINT        NOT NULL,
    month_number                SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    quarter_number              SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    product_line                VARCHAR(50)     NOT NULL,
    lob_code                    VARCHAR(20)     NOT NULL,
    claim_type_code             VARCHAR(20)     NOT NULL,
    state_code                  CHAR(2)         NOT NULL,
    status_code                 VARCHAR(10)     NOT NULL,
    -- Count measures
    claim_count                 INT             NOT NULL DEFAULT 0,
    open_claim_count            INT             NOT NULL DEFAULT 0,
    closed_claim_count          INT             NOT NULL DEFAULT 0,
    litigated_claim_count       INT             NOT NULL DEFAULT 0,
    at_fault_claim_count        INT             NOT NULL DEFAULT 0,
    -- Financial measures
    total_incurred              NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_paid                  NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_reserved              NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    total_outstanding           NUMBER(18,2)    NOT NULL DEFAULT 0,    -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    avg_incurred_per_claim      NUMBER(18,2)    NULL,                  -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    -- Severity / frequency
    avg_settlement_days         NUMBER(10,2)    NULL,                  -- [CONVERT] DECIMAL(10,2) → NUMBER(10,2)
    avg_report_lag_days         NUMBER(10,2)    NULL,                  -- [CONVERT] DECIMAL(10,2) → NUMBER(10,2)
    -- Loss ratio (vs earned premium from agg_policy_monthly)
    earned_premium_ref          NUMBER(18,2)    NULL,                  -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    loss_ratio_pct              NUMBER(10,4)    NULL,                  -- [CONVERT] DECIMAL(10,4) → NUMBER(10,4)
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() -- [CONVERT] DATETIME DEFAULT GETDATE() → TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO agg_claims_monthly (
    year_number, month_number, quarter_number,
    product_line, lob_code, claim_type_code, state_code, status_code,
    claim_count, open_claim_count, closed_claim_count,
    litigated_claim_count, at_fault_claim_count,
    total_incurred, total_paid, total_reserved, total_outstanding,
    avg_incurred_per_claim, avg_settlement_days, avg_report_lag_days
)
SELECT
    dd.year_number,
    dd.month_number,
    dd.quarter_number,
    COALESCE(dp.product_line, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dp.lob_code, 'UNKNOWN'),                      -- [CONVERT] ISNULL → COALESCE
    fc.claim_type_code,
    COALESCE(dc.state_code, 'XX'),                         -- [CONVERT] ISNULL → COALESCE
    fc.status_code,
    COUNT(fc.claim_fact_key),
    SUM(CASE WHEN fc.status_code NOT IN ('CL','WD') THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.status_code = 'CL'             THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.litigation_flag = 'Y'          THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.fault_indicator = 'Y'          THEN 1 ELSE 0 END),
    SUM(COALESCE(fc.total_incurred, 0)),                   -- [CONVERT] ISNULL → COALESCE
    SUM(COALESCE(fc.total_paid, 0)),                       -- [CONVERT] ISNULL → COALESCE
    SUM(COALESCE(fc.total_reserved, 0)),                   -- [CONVERT] ISNULL → COALESCE
    SUM(COALESCE(fc.outstanding_reserve, 0)),              -- [CONVERT] ISNULL → COALESCE
    CASE WHEN COUNT(fc.claim_fact_key) > 0
         THEN ROUND(SUM(COALESCE(fc.total_incurred, 0)) / COUNT(fc.claim_fact_key), 2)
         ELSE NULL END,
    ROUND(AVG(CAST(COALESCE(fc.claim_settlement_days, 0) AS NUMBER(10,2))), 2), -- [CONVERT] ISNULL → COALESCE; DECIMAL → NUMBER
    ROUND(AVG(CAST(COALESCE(fc.claim_report_lag_days, 0) AS NUMBER(10,2))), 2)  -- [CONVERT] ISNULL → COALESCE; DECIMAL → NUMBER
FROM fact_claims fc
INNER JOIN dim_date     dd ON dd.date_key    = fc.incident_date_key
INNER JOIN fact_policy  fp ON fp.policy_id   = fc.policy_id
INNER JOIN dim_product  dp ON dp.product_key = fp.product_key
INNER JOIN dim_customer dc ON dc.customer_key = fc.customer_key
WHERE dd.year_number >= 2015
GROUP BY
    dd.year_number, dd.month_number, dd.quarter_number,
    COALESCE(dp.product_line, 'UNKNOWN'),                  -- [CONVERT] ISNULL → COALESCE
    COALESCE(dp.lob_code, 'UNKNOWN'),                      -- [CONVERT] ISNULL → COALESCE
    fc.claim_type_code,
    COALESCE(dc.state_code, 'XX'),                         -- [CONVERT] ISNULL → COALESCE
    fc.status_code
;

-- Enrich with loss ratio by joining to policy aggregate
-- [CONVERT] Sybase UPDATE...FROM alias syntax → Snowflake UPDATE...SET...FROM syntax
UPDATE agg_claims_monthly acm
SET    acm.earned_premium_ref = apm.total_earned_premium,
       acm.loss_ratio_pct     = CASE WHEN apm.total_earned_premium > 0
                                     THEN ROUND(acm.total_incurred
                                                / apm.total_earned_premium * 100.0, 4)
                                     ELSE NULL END
FROM   agg_policy_monthly apm
WHERE  apm.year_number    = acm.year_number
  AND  apm.month_number   = acm.month_number
  AND  apm.lob_code       = acm.lob_code
  AND  apm.state_code     = acm.state_code
;


-- #############################################################################
-- GROUP 4: v_exec_kpi_summary (sql5.sql lines 635-705)
-- #############################################################################


-- =============================================================================
-- OBJECT 12 of 14: v_exec_kpi_summary
-- Source : sql5.sql lines 635-705
-- Type   : VIEW (reporting)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE VIEW     [CONVERT]
--          GO → removed                                       [REMOVE]
--          PRINT → removed                                    [REMOVE]
-- Note   : No ISNULL/GETDATE in this view; CASE and aggregates are identical
-- =============================================================================

CREATE OR REPLACE VIEW v_exec_kpi_summary AS
SELECT
    apm.year_number,
    apm.quarter_number,
    apm.month_number,
    apm.product_line,
    apm.lob_code,
    apm.region,
    -- Premium KPIs
    SUM(apm.policy_count)                        AS total_policies,
    SUM(apm.total_annual_premium)                AS gwp,
    SUM(apm.total_earned_premium)                AS earned_premium,
    -- Claims KPIs
    SUM(acm.claim_count)                         AS claim_count,
    SUM(acm.total_incurred)                      AS total_losses,
    SUM(acm.total_outstanding)                   AS total_ibnr_reserve,
    -- Ratios
    CASE WHEN SUM(apm.total_earned_premium) > 0
         THEN ROUND(SUM(acm.total_incurred) / SUM(apm.total_earned_premium) * 100, 2)
         ELSE NULL END                           AS loss_ratio_pct,
    CASE WHEN SUM(apm.policy_count) > 0
         THEN ROUND(SUM(acm.claim_count) * 1000.0 / SUM(apm.policy_count), 4)
         ELSE NULL END                           AS claim_frequency_per_1000,
    CASE WHEN SUM(acm.claim_count) > 0
         THEN ROUND(SUM(acm.total_incurred) / SUM(acm.claim_count), 2)
         ELSE NULL END                           AS avg_claim_severity
FROM agg_policy_monthly apm
LEFT OUTER JOIN agg_claims_monthly acm
    ON  acm.year_number  = apm.year_number
    AND acm.month_number = apm.month_number
    AND acm.lob_code     = apm.lob_code
    AND acm.state_code   = apm.state_code
GROUP BY
    apm.year_number, apm.quarter_number, apm.month_number,
    apm.product_line, apm.lob_code, apm.region
;
-- [REMOVE] PRINT 'Aggregate marts and reporting views created successfully' — Sybase PRINT; no Snowflake equivalent


-- #############################################################################
-- GROUP 5: fact_payments (sql3.sql lines 495-639)
-- #############################################################################


-- =============================================================================
-- OBJECT 13 of 14: fact_payments (DDL)
-- Source : sql3.sql lines 495-547
-- Type   : TABLE (fact)
-- Changes: IF EXISTS+DROP+CREATE → CREATE OR REPLACE          [CONVERT]
--          IDENTITY(1,1) → AUTOINCREMENT                      [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                           [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                    [CONVERT]
--          DECIMAL(18,2) → NUMBER(18,2)                       [CONVERT]
--          GO → removed                                       [REMOVE]
-- =============================================================================

CREATE OR REPLACE TABLE fact_payments (
    payment_fact_key            BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY, -- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
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
    payment_amount              NUMBER(18,2)    NOT NULL,   -- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
    days_past_due               INT             NULL,       -- derived
    -- Audit
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(), -- [CONVERT] DATETIME DEFAULT GETDATE() → TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    dw_batch_id                 INT             NULL
);


-- =============================================================================
-- OBJECT 14 of 14: fact_payments (INSERT)
-- Source : sql3.sql lines 551-639
-- Type   : INSERT (fact load)
-- Changes: ISNULL(x, -1) → COALESCE(x, -1)                          [CONVERT]
--          CONVERT(INT,CONVERT(CHAR(8),d,112)) →
--              TO_NUMBER(TO_CHAR(d,'YYYYMMDD'))                       [CONVERT]
--          DATEDIFF(DAY,a,b) → DATEDIFF('DAY',a,b)                   [CONVERT]
--          GO → removed                                               [REMOVE]
-- =============================================================================

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
    COALESCE(dc.customer_key, -1),                         -- [CONVERT] ISNULL(x, -1) → COALESCE(x, -1)
    COALESCE(fp.product_key, -1),                          -- [CONVERT] ISNULL(x, -1) → COALESCE(x, -1)
    TO_NUMBER(TO_CHAR(sp.payment_date, 'YYYYMMDD')),       -- [CONVERT] CONVERT(INT,CONVERT(CHAR(8),d,112)) → TO_NUMBER(TO_CHAR(d,'YYYYMMDD'))
    CASE WHEN sp.due_date IS NOT NULL
         THEN TO_NUMBER(TO_CHAR(sp.due_date, 'YYYYMMDD')) -- [CONVERT] CONVERT(INT,CONVERT(CHAR(8),d,112)) → TO_NUMBER(TO_CHAR(d,'YYYYMMDD'))
         ELSE NULL END,
    sp.payment_method,
    sp.payment_status,
    sp.installment_number,
    sp.late_flag,
    sp.nsf_flag,
    sp.payment_amount,
    CASE WHEN sp.due_date IS NOT NULL AND sp.payment_date > sp.due_date
         THEN DATEDIFF('DAY', sp.due_date, sp.payment_date) -- [CONVERT] DATEDIFF(DAY,a,b) → DATEDIFF('DAY',a,b)
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
;


-- =============================================================================
-- END OF MEDIUM-TIER CONVERTED OBJECTS
-- Total objects: 14
--   Group 1 (Staging DDL):  9 tables
--   Group 2 (Staging DML):  6 INSERT statements (covering policies, agents,
--                            customers, territories, reinsurance, underwriters)
--   Group 3 (Aggregates):   2 tables (agg_policy_monthly, agg_claims_monthly)
--                            with INSERT + UPDATE
--   Group 4 (View):         1 view (v_exec_kpi_summary)
--   Group 5 (Fact):         1 table + INSERT (fact_payments)
--
-- Notes:
--   - stg_claims, stg_products, stg_payments INSERT DML not present in source
--     ETL_Script_01.sql; only DDL was found. DML may reside in a separate script.
--   - All OperationalDB.dbo.* linked-server references have been replaced with
--     bare table names and annotated with [TODO] comments for Snowflake source mapping.
--   - Sybase domain objects (CREATE RULE, CREATE DEFAULT, sp_bindrule,
--     sp_binddefault) have been inlined as CHECK constraints and column DEFAULTs.
--   - All DECIMAL types converted to NUMBER per specification.
-- =============================================================================
