-- =============================================================================
-- ETL SCRIPT 01 (ENHANCED): Stage Raw Source Data
-- Description : Extract from 9 operational source tables into staging area
--               for downstream transformation into star schema.
-- Source DB   : OperationalDB (Sybase ASE) via linked server
-- Target DB   : DataWarehouse (Sybase IQ)
-- Author      : DW Team
-- Created     : 2009-03-15
-- Modified    : 2014-07-22
-- Complexity  : MEDIUM — IQ-specific DDL, RULE/DEFAULT objects, cross-DB refs
-- =============================================================================

-- =============================================================================
-- SECTION A: IQ Session Tuning (IQ-specific — no Snowflake equivalent)
-- =============================================================================

SET TEMPORARY OPTION Query_Plan = 'ON'
GO
SET TEMPORARY OPTION Optimization_Goal = 'AllRows'
GO
SET TEMPORARY OPTION Prefetch = 'ON'
GO

-- =============================================================================
-- SECTION B: Domain Rules and Defaults
-- These are Sybase-specific object types with no direct Snowflake equivalent.
-- Migration: inline as CHECK constraints and column DEFAULT values.
-- =============================================================================

-- Rule: status codes must be 2 chars from approved set
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_status_code' AND type = 'R')
    DROP RULE rule_status_code
GO
CREATE RULE rule_status_code
AS @value IN ('AC','IN','CN','PD','SU','EX','RN','LO')
GO

-- Rule: premium amounts must be positive or null
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_positive_amount' AND type = 'R')
    DROP RULE rule_positive_amount
GO
CREATE RULE rule_positive_amount
AS @value IS NULL OR @value >= 0.00
GO

-- Rule: flag columns must be Y or N
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_yn_flag' AND type = 'R')
    DROP RULE rule_yn_flag
GO
CREATE RULE rule_yn_flag
AS @value IN ('Y', 'N')
GO

-- Rule: state codes must be 2 uppercase chars
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_state_code' AND type = 'R')
    DROP RULE rule_state_code
GO
CREATE RULE rule_state_code
AS @value LIKE '[A-Z][A-Z]'
GO

-- Default: extract timestamp
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_extract_ts' AND type = 'D')
    DROP DEFAULT def_extract_ts
GO
CREATE DEFAULT def_extract_ts
AS GETDATE()
GO

-- Default: active flag
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_active_flag' AND type = 'D')
    DROP DEFAULT def_active_flag
GO
CREATE DEFAULT def_active_flag
AS 'Y'
GO

-- Default: zero amount
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_zero_amount' AND type = 'D')
    DROP DEFAULT def_zero_amount
GO
CREATE DEFAULT def_zero_amount
AS 0.00
GO

-- =============================================================================
-- SECTION C: Drop existing staging tables
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_policies'      AND type = 'U') DROP TABLE stg_policies
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_customers'     AND type = 'U') DROP TABLE stg_customers
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_claims'        AND type = 'U') DROP TABLE stg_claims
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_agents'        AND type = 'U') DROP TABLE stg_agents
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_products'      AND type = 'U') DROP TABLE stg_products
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_payments'      AND type = 'U') DROP TABLE stg_payments
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_territories'   AND type = 'U') DROP TABLE stg_territories
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_reinsurance'   AND type = 'U') DROP TABLE stg_reinsurance
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_underwriters'  AND type = 'U') DROP TABLE stg_underwriters
GO

-- =============================================================================
-- SECTION D: Create staging tables
-- IQ-specific: IN DBASPACE, IQ UNIQUE — no Snowflake equivalent for storage
-- =============================================================================

CREATE TABLE stg_policies (
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
    renewal_count       TINYINT         NOT NULL DEFAULT 0,
    cancellation_reason VARCHAR(50)     NULL,
    created_date        DATETIME        NOT NULL,
    last_updated        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

-- Bind rules and defaults to stg_policies columns
EXEC sp_bindrule  'rule_status_code',   'stg_policies.status_code'
EXEC sp_bindrule  'rule_positive_amount','stg_policies.annual_premium'
EXEC sp_binddefault 'def_extract_ts',   'stg_policies.src_extract_ts'
GO

-- IQ UNIQUE: columnar cardinality hint (no equivalent in Snowflake)
ALTER TABLE stg_policies ADD CONSTRAINT uq_stg_policy_id IQ UNIQUE(500000)
GO

CREATE TABLE stg_customers (
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
    lifetime_value      MONEY           NULL,      -- MONEY type: migrate to NUMBER(19,4)
    churn_risk_score    DECIMAL(5,4)    NULL,
    is_vip              CHAR(1)         NOT NULL DEFAULT 'N',
    created_date        DATETIME        NOT NULL,
    last_updated        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',      'stg_customers.is_vip'
EXEC sp_bindrule    'rule_state_code',   'stg_customers.state_code'
EXEC sp_binddefault 'def_active_flag',   'stg_customers.is_vip'
EXEC sp_binddefault 'def_extract_ts',    'stg_customers.src_extract_ts'
GO

CREATE TABLE stg_claims (
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
    total_incurred      DECIMAL(18,2)   NULL,
    total_paid          DECIMAL(18,2)   NULL,
    total_reserved      DECIMAL(18,2)   NULL,
    salvage_amount      DECIMAL(18,2)   NULL,
    subrogation_amount  DECIMAL(18,2)   NULL,
    adjuster_id         INT             NULL,
    fault_indicator     CHAR(1)         NULL,
    litigation_flag     CHAR(1)         NULL,
    catastrophe_code    VARCHAR(10)     NULL,
    reinsurance_flag    CHAR(1)         NOT NULL DEFAULT 'N',
    created_date        DATETIME        NOT NULL,
    last_updated        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',      'stg_claims.litigation_flag'
EXEC sp_bindrule    'rule_yn_flag',      'stg_claims.reinsurance_flag'
EXEC sp_binddefault 'def_active_flag',   'stg_claims.reinsurance_flag'
EXEC sp_binddefault 'def_extract_ts',    'stg_claims.src_extract_ts'
GO

CREATE TABLE stg_agents (
    agent_id            INT             NOT NULL,
    agent_number        VARCHAR(20)     NOT NULL,
    agent_name          VARCHAR(200)    NOT NULL,
    agency_id           INT             NULL,
    agency_name         VARCHAR(200)    NULL,
    parent_agency_id    INT             NULL,      -- Self-referencing hierarchy
    hierarchy_level     TINYINT         NULL,      -- 1=Direct, 2=Agency, 3=MGA
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
    ytd_premium_written MONEY           NULL,
    created_date        DATETIME        NOT NULL,
    last_updated        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',      'stg_agents.active_flag'
EXEC sp_binddefault 'def_active_flag',   'stg_agents.active_flag'
EXEC sp_binddefault 'def_extract_ts',    'stg_agents.src_extract_ts'
GO

CREATE TABLE stg_products (
    product_code        VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(200)    NOT NULL,
    product_line        VARCHAR(50)     NULL,
    lob_code            VARCHAR(10)     NULL,
    lob_description     VARCHAR(100)    NULL,
    coverage_type       VARCHAR(50)     NULL,
    base_rate           DECIMAL(10,6)   NULL,
    min_premium         MONEY           NULL,
    max_coverage        MONEY           NULL,
    is_commercial       CHAR(1)         NOT NULL DEFAULT 'N',
    is_reinsurable      CHAR(1)         NOT NULL DEFAULT 'Y',
    filing_state        CHAR(2)         NULL,
    effective_date      DATE            NOT NULL,
    expiry_date         DATE            NULL,
    created_date        DATETIME        NOT NULL,
    last_updated        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',      'stg_products.is_commercial'
EXEC sp_bindrule    'rule_yn_flag',      'stg_products.is_reinsurable'
EXEC sp_binddefault 'def_extract_ts',    'stg_products.src_extract_ts'
GO

CREATE TABLE stg_payments (
    payment_id          BIGINT          NOT NULL,
    payment_reference   VARCHAR(40)     NOT NULL,
    policy_id           BIGINT          NOT NULL,
    claim_id            BIGINT          NULL,
    payment_date        DATE            NOT NULL,
    payment_type_code   VARCHAR(20)     NOT NULL,
    payment_method      VARCHAR(20)     NULL,
    payment_amount      MONEY           NOT NULL,  -- MONEY: migrate to NUMBER(19,4)
    applied_amount      MONEY           NULL,
    reversal_flag       CHAR(1)         NOT NULL DEFAULT 'N',
    reversal_date       DATE            NULL,
    reversal_reason     VARCHAR(200)    NULL,
    check_number        VARCHAR(30)     NULL,
    bank_routing        VARCHAR(9)      NULL,
    created_date        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',      'stg_payments.reversal_flag'
EXEC sp_bindrule    'rule_positive_amount','stg_payments.payment_amount'
EXEC sp_binddefault 'def_extract_ts',    'stg_payments.src_extract_ts'
GO

-- NEW: Territory hierarchy staging
CREATE TABLE stg_territories (
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
    created_date        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_state_code',   'stg_territories.state_code'
EXEC sp_bindrule    'rule_yn_flag',      'stg_territories.wind_pool_flag'
EXEC sp_bindrule    'rule_yn_flag',      'stg_territories.active_flag'
EXEC sp_binddefault 'def_extract_ts',    'stg_territories.src_extract_ts'
GO

-- NEW: Reinsurance cession staging
CREATE TABLE stg_reinsurance (
    cession_id          BIGINT          NOT NULL,
    treaty_code         VARCHAR(20)     NOT NULL,
    treaty_type         VARCHAR(20)     NULL,   -- QS, XL, FAC
    policy_id           BIGINT          NOT NULL,
    claim_id            BIGINT          NULL,
    cedant_amount       DECIMAL(18,2)   NOT NULL,
    reinsurer_share_pct DECIMAL(7,4)    NOT NULL,
    reinsurer_amount    DECIMAL(18,2)   NOT NULL,
    reinsurer_code      VARCHAR(20)     NOT NULL,
    layer_number        TINYINT         NOT NULL DEFAULT 1,
    retention_amount    DECIMAL(18,2)   NULL,
    reinstatement_flag  CHAR(1)         NOT NULL DEFAULT 'N',
    settlement_date     DATE            NULL,
    period_start        DATE            NOT NULL,
    period_end          DATE            NOT NULL,
    created_date        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_binddefault 'def_extract_ts',   'stg_reinsurance.src_extract_ts'
GO

-- NEW: Underwriter staging
CREATE TABLE stg_underwriters (
    underwriter_id      INT             NOT NULL,
    underwriter_code    VARCHAR(20)     NOT NULL,
    underwriter_name    VARCHAR(200)    NOT NULL,
    department_code     VARCHAR(20)     NULL,
    authority_level     VARCHAR(10)     NULL,   -- JR, SR, EXEC
    max_line_amount     MONEY           NULL,
    lob_speciality      VARCHAR(50)     NULL,
    license_state       CHAR(2)         NULL,
    active_flag         CHAR(1)         NOT NULL DEFAULT 'Y',
    start_date          DATE            NOT NULL,
    end_date            DATE            NULL,
    created_date        DATETIME        NOT NULL,
    src_extract_ts      DATETIME        NULL
) IN DBASPACE
GO

EXEC sp_bindrule    'rule_yn_flag',     'stg_underwriters.active_flag'
EXEC sp_binddefault 'def_active_flag',  'stg_underwriters.active_flag'
EXEC sp_binddefault 'def_extract_ts',   'stg_underwriters.src_extract_ts'
GO

-- =============================================================================
-- SECTION E: Populate staging tables via cross-DB linked server queries
-- Sybase three-part name: DatabaseName.OwnerName.TableName
-- Migration note: replace with Snowflake COPY INTO from external stage or
--                 Fivetran connector. No direct linked-server equivalent.
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
    LTRIM(RTRIM(p.policy_no)),
    p.cust_id,
    p.agent_id,
    UPPER(LTRIM(RTRIM(p.product_cd))),
    CONVERT(DATE, p.eff_dt,  112),   -- style 112: YYYYMMDD
    CONVERT(DATE, p.exp_dt,  112),
    UPPER(LTRIM(RTRIM(p.status_cd))),
    p.annual_prem,
    p.coverage_amt,
    LTRIM(RTRIM(p.terr_cd)),
    p.underwriter_id,
    ISNULL(p.renewal_cnt, 0),
    NULLIF(LTRIM(RTRIM(p.cancel_rsn)), ''),
    p.create_dt,
    p.upd_dt
FROM OperationalDB.dbo.ins_policy p         -- THREE-PART NAME: linked server
WHERE p.upd_dt >= DATEADD(day, -90, GETDATE())
   OR p.status_cd IN ('AC', 'RN')           -- All active/renewal regardless of date
GO

-- Full-refresh agents (small table)
TRUNCATE TABLE stg_agents
GO

INSERT INTO stg_agents (
    agent_id, agent_number, agent_name, agency_id, agency_name,
    parent_agency_id, hierarchy_level, license_state, license_number, license_expiry,
    region_code, district_code, channel_code, commission_tier,
    active_flag, hire_date, termination_date, ytd_premium_written,
    created_date, last_updated
)
SELECT
    a.agent_id,
    LTRIM(RTRIM(a.agent_no)),
    LTRIM(RTRIM(a.agent_nm)),
    a.agency_id,
    LTRIM(RTRIM(ag.agency_nm)),
    ag.parent_agency_id,
    CASE
        WHEN ag.parent_agency_id IS NULL THEN 1
        WHEN ag2.parent_agency_id IS NULL THEN 2
        ELSE 3
    END,
    UPPER(LTRIM(RTRIM(a.lic_state))),
    LTRIM(RTRIM(a.lic_no)),
    CONVERT(DATE, a.lic_exp_dt, 112),
    LTRIM(RTRIM(a.region_cd)),
    LTRIM(RTRIM(a.district_cd)),
    UPPER(LTRIM(RTRIM(a.channel_cd))),
    ISNULL(LTRIM(RTRIM(a.comm_tier)), 'STD'),
    CASE WHEN a.term_dt IS NULL OR a.term_dt > GETDATE() THEN 'Y' ELSE 'N' END,
    CONVERT(DATE, a.hire_dt,  112),
    CASE WHEN a.term_dt IS NOT NULL THEN CONVERT(DATE, a.term_dt, 112) ELSE NULL END,
    a.ytd_prem,
    a.create_dt,
    a.upd_dt
FROM OperationalDB.dbo.agent_master a                           -- THREE-PART NAME
LEFT JOIN OperationalDB.dbo.agency_master ag ON ag.agency_id = a.agency_id
LEFT JOIN OperationalDB.dbo.agency_master ag2 ON ag2.agency_id = ag.parent_agency_id
GO

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
    LTRIM(RTRIM(c.first_nm)),
    LTRIM(RTRIM(c.last_nm)),
    CONVERT(DATE, c.dob, 112),
    UPPER(LTRIM(RTRIM(c.gender_cd))),
    UPPER(LTRIM(RTRIM(c.marital_cd))),
    LTRIM(RTRIM(c.addr1)),
    NULLIF(LTRIM(RTRIM(c.addr2)), ''),
    LTRIM(RTRIM(c.city_nm)),
    UPPER(LTRIM(RTRIM(c.state_cd))),
    LTRIM(RTRIM(c.zip)),
    LOWER(LTRIM(RTRIM(c.email_addr))),
    LTRIM(RTRIM(c.phone1)),
    NULLIF(LTRIM(RTRIM(c.phone2)), ''),
    c.credit_score,
    CASE
        WHEN c.credit_score >= 800 THEN 'EXCEPTIONAL'
        WHEN c.credit_score >= 740 THEN 'VERY_GOOD'
        WHEN c.credit_score >= 670 THEN 'GOOD'
        WHEN c.credit_score >= 580 THEN 'FAIR'
        WHEN c.credit_score IS NOT NULL THEN 'POOR'
        ELSE 'UNKNOWN'
    END,
    ISNULL(LTRIM(RTRIM(c.segment_cd)), 'STANDARD'),
    LTRIM(RTRIM(c.acq_source)),
    c.ltv_amount,
    c.churn_score,
    CASE WHEN c.ltv_amount > 50000 THEN 'Y' ELSE 'N' END,
    c.create_dt,
    c.upd_dt
FROM OperationalDB.dbo.customer_master c                        -- THREE-PART NAME
WHERE c.upd_dt >= DATEADD(day, -90, GETDATE())
GO

-- Full-refresh territory (small reference table)
TRUNCATE TABLE stg_territories
GO

INSERT INTO stg_territories (
    territory_code, territory_name, state_code, region_code, region_name,
    zone_code, zone_name, catastrophe_zone, wind_pool_flag, active_flag,
    effective_date, expiry_date, created_date
)
SELECT
    UPPER(LTRIM(RTRIM(t.terr_cd))),
    LTRIM(RTRIM(t.terr_nm)),
    UPPER(LTRIM(RTRIM(t.state_cd))),
    UPPER(LTRIM(RTRIM(t.region_cd))),
    LTRIM(RTRIM(r.region_nm)),
    UPPER(LTRIM(RTRIM(t.zone_cd))),
    LTRIM(RTRIM(z.zone_nm)),
    LTRIM(RTRIM(t.cat_zone)),
    ISNULL(t.wind_pool_flag, 'N'),
    CASE WHEN t.exp_dt IS NULL OR t.exp_dt > GETDATE() THEN 'Y' ELSE 'N' END,
    CONVERT(DATE, t.eff_dt, 112),
    CASE WHEN t.exp_dt IS NOT NULL THEN CONVERT(DATE, t.exp_dt, 112) ELSE NULL END,
    t.create_dt
FROM OperationalDB.dbo.territory_master t                       -- THREE-PART NAME
LEFT JOIN OperationalDB.dbo.region_ref r     ON r.region_cd  = t.region_cd
LEFT JOIN OperationalDB.dbo.zone_ref z       ON z.zone_cd    = t.zone_cd
GO

-- Incremental reinsurance cessions
INSERT INTO stg_reinsurance (
    cession_id, treaty_code, treaty_type, policy_id, claim_id,
    cedant_amount, reinsurer_share_pct, reinsurer_amount,
    reinsurer_code, layer_number, retention_amount, reinstatement_flag,
    settlement_date, period_start, period_end, created_date
)
SELECT
    r.cession_id,
    LTRIM(RTRIM(r.treaty_cd)),
    UPPER(LTRIM(RTRIM(r.treaty_type))),
    r.policy_id,
    r.claim_id,
    r.cedant_amt,
    r.reins_share_pct / 100.0,   -- Source stores as percentage integer
    r.reins_amt,
    UPPER(LTRIM(RTRIM(r.reins_cd))),
    ISNULL(r.layer_no, 1),
    r.retention_amt,
    ISNULL(r.reinstatement_flag, 'N'),
    CASE WHEN r.settle_dt IS NOT NULL THEN CONVERT(DATE, r.settle_dt, 112) ELSE NULL END,
    CONVERT(DATE, r.period_start, 112),
    CONVERT(DATE, r.period_end,   112),
    r.create_dt
FROM OperationalDB.dbo.reinsurance_cessions r                   -- THREE-PART NAME
WHERE r.create_dt >= DATEADD(day, -90, GETDATE())
GO

-- Full-refresh underwriters
TRUNCATE TABLE stg_underwriters
GO

INSERT INTO stg_underwriters (
    underwriter_id, underwriter_code, underwriter_name, department_code,
    authority_level, max_line_amount, lob_speciality, license_state,
    active_flag, start_date, end_date, created_date
)
SELECT
    u.uw_id,
    UPPER(LTRIM(RTRIM(u.uw_cd))),
    LTRIM(RTRIM(u.uw_nm)),
    LTRIM(RTRIM(u.dept_cd)),
    UPPER(LTRIM(RTRIM(u.auth_level))),
    u.max_line_amt,
    LTRIM(RTRIM(u.lob_spec)),
    UPPER(LTRIM(RTRIM(u.lic_state))),
    CASE WHEN u.end_dt IS NULL OR u.end_dt > GETDATE() THEN 'Y' ELSE 'N' END,
    CONVERT(DATE, u.start_dt, 112),
    CASE WHEN u.end_dt IS NOT NULL THEN CONVERT(DATE, u.end_dt, 112) ELSE NULL END,
    u.create_dt
FROM OperationalDB.dbo.underwriter_master u                     -- THREE-PART NAME
GO

PRINT 'Script 01 Complete: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' underwriter rows staged'
GO
