-- ============================================================================= 

-- ETL SCRIPT 01: Stage Raw Source Data 

-- Description : Extract from 6 operational source tables into staging area 

--               for downstream transformation into star schema. 

-- Source DB   : OperationalDB (Sybase ASE) 

-- Target DB   : DataWarehouse (Sybase IQ) 

-- Author      : DW Team 

-- Created     : 2009-03-15 

-- Modified    : 2012-11-02 

-- ============================================================================= 

  

-- ------------------------------------------------------- 

-- Drop and recreate staging tables 

-- ------------------------------------------------------- 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_policies' AND type = 'U') 

    DROP TABLE stg_policies 

GO 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_customers' AND type = 'U') 

    DROP TABLE stg_customers 

GO 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_claims' AND type = 'U') 

    DROP TABLE stg_claims 

GO 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_agents' AND type = 'U') 

    DROP TABLE stg_agents 

GO 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_products' AND type = 'U') 

    DROP TABLE stg_products 

GO 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'stg_payments' AND type = 'U') 

    DROP TABLE stg_payments 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Policies 

-- ------------------------------------------------------- 

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

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Customers 

-- ------------------------------------------------------- 

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

    credit_score        SMALLINT        NULL, 

    customer_segment    VARCHAR(30)     NULL, 

    acquisition_source  VARCHAR(50)     NULL, 

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Claims 

-- ------------------------------------------------------- 

CREATE TABLE stg_claims ( 

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

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Agents 

-- ------------------------------------------------------- 

CREATE TABLE stg_agents ( 

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

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Products 

-- ------------------------------------------------------- 

CREATE TABLE stg_products ( 

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

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Create Staging: Payments 

-- ------------------------------------------------------- 

CREATE TABLE stg_payments ( 

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

    created_date        DATETIME        NOT NULL, 

    last_updated        DATETIME        NOT NULL, 

    src_extract_ts      DATETIME        DEFAULT GETDATE() 

) 

GO 

  

-- ------------------------------------------------------- 

-- Populate staging tables from operational source 

-- Uses linked server / remote query syntax for Sybase IQ 

-- ------------------------------------------------------- 

  

-- Extract Policies (incremental: last 90 days + all open) 

INSERT INTO DataWarehouse..stg_policies 

SELECT 

    p.policy_id, 

    p.policy_number, 

    p.customer_id, 

    p.agent_id, 

    p.product_code, 

    CONVERT(DATE, p.effective_date), 

    CONVERT(DATE, p.expiry_date), 

    p.status_code, 

    p.annual_premium, 

    p.coverage_amount, 

    p.territory_code, 

    p.underwriter_id, 

    p.created_date, 

    p.last_updated, 

    GETDATE() 

FROM OperationalDB..ins_policy p 

WHERE p.last_updated >= DATEADD(DAY, -90, GETDATE()) 

   OR p.status_code IN ('AC', 'PE')   -- Active or Pending always refreshed 

GO 

  

-- Extract Customers (changed in last 90 days) 

INSERT INTO DataWarehouse..stg_customers 

SELECT 

    c.customer_id, 

    LTRIM(RTRIM(c.first_name)), 

    LTRIM(RTRIM(c.last_name)), 

    CONVERT(DATE, c.dob), 

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

    GETDATE() 

FROM OperationalDB..customer_master c 

WHERE c.modified_ts >= DATEADD(DAY, -90, GETDATE()) 

GO 

  

-- Extract Claims (open claims + recently modified) 

INSERT INTO DataWarehouse..stg_claims 

SELECT 

    cl.claim_id, 

    cl.claim_no, 

    cl.policy_id, 

    cl.insured_customer_id, 

    CONVERT(DATE, cl.loss_date), 

    CONVERT(DATE, cl.report_date), 

    CONVERT(DATE, cl.close_date), 

    cl.claim_type, 

    cl.claim_status, 

    ISNULL(cl.total_incurred, 0.00), 

    ISNULL(cl.paid_amount, 0.00), 

    ISNULL(cl.reserve_amount, 0.00), 

    cl.adjuster_id, 

    cl.at_fault_ind, 

    cl.litigation_ind, 

    cl.created_ts, 

    cl.modified_ts, 

    GETDATE() 

FROM OperationalDB..claims_header cl 

WHERE cl.claim_status NOT IN ('CL','WD')     -- Not Closed or Withdrawn 

   OR cl.modified_ts >= DATEADD(DAY, -90, GETDATE()) 

GO 

  

-- Extract Agents (full refresh - relatively small table) 

INSERT INTO DataWarehouse..stg_agents 

SELECT 

    a.agent_id, 

    a.agent_no, 

    a.agent_full_name, 

    a.agency_id, 

    ag.agency_name, 

    a.lic_state, 

    a.lic_number, 

    CONVERT(DATE, a.lic_expiry_dt), 

    a.region_cd, 

    a.distribution_channel, 

    a.active_ind, 

    CONVERT(DATE, a.hire_dt), 

    CONVERT(DATE, a.term_dt), 

    a.create_ts, 

    a.update_ts, 

    GETDATE() 

FROM OperationalDB..agent_master a 

LEFT OUTER JOIN OperationalDB..agency_master ag 

    ON a.agency_id = ag.agency_id 

GO 

  

-- Extract Products (full refresh - reference table) 

INSERT INTO DataWarehouse..stg_products 

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

    CONVERT(DATE, pr.eff_dt), 

    CONVERT(DATE, pr.disc_dt), 

    pr.create_ts, 

    pr.update_ts, 

    GETDATE() 

FROM OperationalDB..product_master pr 

GO 

  

-- Extract Payments (last 90 days) 

INSERT INTO DataWarehouse..stg_payments 

SELECT 

    pmt.payment_id, 

    pmt.policy_id, 

    pmt.customer_id, 

    CONVERT(DATE, pmt.payment_dt), 

    CONVERT(DATE, pmt.due_dt), 

    pmt.payment_amt, 

    pmt.payment_method_cd, 

    pmt.payment_status_cd, 

    pmt.invoice_no, 

    pmt.installment_no, 

    pmt.late_ind, 

    pmt.nsf_ind, 

    pmt.create_ts, 

    pmt.update_ts, 

    GETDATE() 

FROM OperationalDB..payment_transactions pmt 

WHERE pmt.payment_dt >= DATEADD(DAY, -90, GETDATE()) 

GO 

  

-- ------------------------------------------------------- 

-- Basic row count validation 

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

SELECT 'stg_payments',                 COUNT(*)              FROM stg_payments 

GO 

  
