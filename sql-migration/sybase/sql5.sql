-- ============================================================================= 

-- ETL SCRIPT 05: Build Aggregate Summary Mart & Reporting Views 

-- Description : Rolls up fact data into monthly summary aggregates for 

--               BI/reporting layer. Builds views that join facts to dims 

--               for use by Cognos / Business Objects / ad-hoc query users. 

-- Target DB   : DataWarehouse (Sybase IQ) 

-- Author      : DW Team 

-- Created     : 2011-03-08 

-- Modified    : 2015-01-14 

-- ============================================================================= 

  

-- ============================================================ 

-- SECTION 1: Monthly Policy Summary (aggregate mart) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'agg_policy_monthly' AND type = 'U') 

    DROP TABLE agg_policy_monthly 

GO 

  

CREATE TABLE agg_policy_monthly ( 

    agg_key                 INT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    -- Grain dimensions 

    year_number             SMALLINT        NOT NULL, 

    month_number            TINYINT         NOT NULL, 

    quarter_number          TINYINT         NOT NULL, 

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

    total_annual_premium    DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    total_earned_premium    DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    total_coverage_amount   DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    avg_premium_per_policy  DECIMAL(18,2)   NULL, 

    -- Renewal / retention metrics 

    renewed_policy_count    INT             NOT NULL DEFAULT 0, 

    -- Audit 

    dw_insert_ts            DATETIME        NOT NULL DEFAULT GETDATE() 

) 

GO 

  

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

    ISNULL(dp.product_line, 'UNKNOWN'), 

    ISNULL(dp.lob_code,     'UNKNOWN'), 

    ISNULL(da.channel_code, 'UNKNOWN'), 

    ISNULL(dc.region,       'UNKNOWN'), 

    ISNULL(dc.state_code,   'XX'), 

    fp.status_code, 

    COUNT(fp.policy_fact_key)                   AS policy_count, 

    SUM(CASE WHEN dd_eff.year_number  = dd_eff.year_number 

              AND dd_eff.month_number = dd_eff.month_number 

             THEN 1 ELSE 0 END)                 AS new_policy_count,    -- simplified 

    SUM(CASE WHEN fp.status_code = 'CN' THEN 1 ELSE 0 END) AS cancelled_count, 

    SUM(ISNULL(fp.annual_premium,        0))    AS total_annual_premium, 

    SUM(ISNULL(fp.earned_premium_amount, 0))    AS total_earned_premium, 

    SUM(ISNULL(fp.coverage_amount,       0))    AS total_coverage_amount, 

    CASE WHEN COUNT(fp.policy_fact_key) > 0 

         THEN ROUND(SUM(ISNULL(fp.annual_premium, 0)) 

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

    ISNULL(dp.product_line, 'UNKNOWN'), 

    ISNULL(dp.lob_code,     'UNKNOWN'), 

    ISNULL(da.channel_code, 'UNKNOWN'), 

    ISNULL(dc.region,       'UNKNOWN'), 

    ISNULL(dc.state_code,   'XX'), 

    fp.status_code 

GO 

  

-- ============================================================ 

-- SECTION 2: Monthly Claims Summary 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'agg_claims_monthly' AND type = 'U') 

    DROP TABLE agg_claims_monthly 

GO 

  

CREATE TABLE agg_claims_monthly ( 

    agg_key                     INT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    year_number                 SMALLINT        NOT NULL, 

    month_number                TINYINT         NOT NULL, 

    quarter_number              TINYINT         NOT NULL, 

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

    total_incurred              DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    total_paid                  DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    total_reserved              DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    total_outstanding           DECIMAL(18,2)   NOT NULL DEFAULT 0, 

    avg_incurred_per_claim      DECIMAL(18,2)   NULL, 

    -- Severity / frequency 

    avg_settlement_days         DECIMAL(10,2)   NULL, 

    avg_report_lag_days         DECIMAL(10,2)   NULL, 

    -- Loss ratio (vs earned premium from agg_policy_monthly) 

    earned_premium_ref          DECIMAL(18,2)   NULL, 

    loss_ratio_pct              DECIMAL(10,4)   NULL, 

    dw_insert_ts                DATETIME        NOT NULL DEFAULT GETDATE() 

) 

GO 

  

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

    ISNULL(dp.product_line, 'UNKNOWN'), 

    ISNULL(dp.lob_code,     'UNKNOWN'), 

    fc.claim_type_code, 

    ISNULL(dc.state_code,   'XX'), 

    fc.status_code, 

    COUNT(fc.claim_fact_key), 

    SUM(CASE WHEN fc.status_code NOT IN ('CL','WD') THEN 1 ELSE 0 END), 

    SUM(CASE WHEN fc.status_code = 'CL'             THEN 1 ELSE 0 END), 

    SUM(CASE WHEN fc.litigation_flag = 'Y'          THEN 1 ELSE 0 END), 

    SUM(CASE WHEN fc.fault_indicator = 'Y'          THEN 1 ELSE 0 END), 

    SUM(ISNULL(fc.total_incurred,    0)), 

    SUM(ISNULL(fc.total_paid,        0)), 

    SUM(ISNULL(fc.total_reserved,    0)), 

    SUM(ISNULL(fc.outstanding_reserve, 0)), 

    CASE WHEN COUNT(fc.claim_fact_key) > 0 

         THEN ROUND(SUM(ISNULL(fc.total_incurred, 0)) / COUNT(fc.claim_fact_key), 2) 

         ELSE NULL END, 

    ROUND(AVG(CAST(ISNULL(fc.claim_settlement_days, 0) AS DECIMAL(10,2))), 2), 

    ROUND(AVG(CAST(ISNULL(fc.claim_report_lag_days, 0) AS DECIMAL(10,2))), 2) 

FROM fact_claims fc 

INNER JOIN dim_date     dd ON dd.date_key    = fc.incident_date_key 

INNER JOIN fact_policy  fp ON fp.policy_id   = fc.policy_id 

INNER JOIN dim_product  dp ON dp.product_key = fp.product_key 

INNER JOIN dim_customer dc ON dc.customer_key = fc.customer_key 

WHERE dd.year_number >= 2015 

GROUP BY 

    dd.year_number, dd.month_number, dd.quarter_number, 

    ISNULL(dp.product_line, 'UNKNOWN'), 

    ISNULL(dp.lob_code,     'UNKNOWN'), 

    fc.claim_type_code, 

    ISNULL(dc.state_code,   'XX'), 

    fc.status_code 

GO 

  

-- Enrich with loss ratio by joining to policy aggregate 

UPDATE acm 

SET    acm.earned_premium_ref = apm.total_earned_premium, 

       acm.loss_ratio_pct     = CASE WHEN apm.total_earned_premium > 0 

                                     THEN ROUND(acm.total_incurred 

                                                / apm.total_earned_premium * 100.0, 4) 

                                     ELSE NULL END 

FROM   agg_claims_monthly acm 

INNER JOIN agg_policy_monthly apm 

    ON  apm.year_number    = acm.year_number 

    AND apm.month_number   = acm.month_number 

    AND apm.lob_code       = acm.lob_code 

    AND apm.state_code     = acm.state_code 

GO 

  

-- ============================================================ 

-- SECTION 3: Reporting Views (used by BI layer) 

-- ============================================================ 

  

-- View: Full policy detail with all dimension attributes resolved 

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'v_policy_detail' AND type = 'V') 

    DROP VIEW v_policy_detail 

GO 

  

CREATE VIEW v_policy_detail AS 

SELECT 

    fp.policy_id, 

    fp.policy_number, 

    fp.status_code, 

    fp.territory_code, 

    fp.annual_premium, 

    fp.coverage_amount, 

    fp.earned_premium_amount, 

    fp.policy_term_days, 

    -- Customer attributes 

    dc.customer_id, 

    dc.full_name             AS customer_name, 

    dc.age_band, 

    dc.gender_description, 

    dc.marital_description, 

    dc.city, 

    dc.state_code, 

    dc.state_name, 

    dc.region, 

    dc.credit_band, 

    dc.customer_segment, 

    dc.acquisition_source, 

    -- Agent attributes 

    da.agent_id, 

    da.agent_name, 

    da.agency_name, 

    da.region_description    AS agent_region, 

    da.channel_description   AS distribution_channel, 

    -- Product attributes 

    dp.product_code, 

    dp.product_name, 

    dp.product_line, 

    dp.lob_code, 

    dp.lob_description, 

    dp.coverage_type, 

    -- Date attributes (effective) 

    dd_eff.calendar_date     AS effective_date, 

    dd_eff.year_number       AS eff_year, 

    dd_eff.month_name        AS eff_month, 

    dd_eff.quarter_name      AS eff_quarter, 

    dd_eff.fiscal_year       AS eff_fiscal_year, 

    -- Date attributes (expiry) 

    dd_exp.calendar_date     AS expiry_date, 

    dd_exp.year_number       AS exp_year 

FROM fact_policy fp 

INNER JOIN dim_customer dc  ON dc.customer_key  = fp.customer_key 

INNER JOIN dim_agent    da  ON da.agent_key     = fp.agent_key 

INNER JOIN dim_product  dp  ON dp.product_key   = fp.product_key 

INNER JOIN dim_date  dd_eff ON dd_eff.date_key  = fp.effective_date_key 

INNER JOIN dim_date  dd_exp ON dd_exp.date_key  = fp.expiry_date_key 

GO 

  

-- View: Claims with full context for loss analysis 

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'v_claims_analysis' AND type = 'V') 

    DROP VIEW v_claims_analysis 

GO 

  

CREATE VIEW v_claims_analysis AS 

SELECT 

    fc.claim_id, 

    fc.claim_number, 

    fc.policy_id, 

    fc.claim_type_code, 

    fc.status_code           AS claim_status, 

    fc.fault_indicator, 

    fc.litigation_flag, 

    fc.total_incurred, 

    fc.total_paid, 

    fc.total_reserved, 

    fc.outstanding_reserve, 

    fc.claim_report_lag_days, 

    fc.claim_settlement_days, 

    fc.loss_ratio_pct, 

    -- Customer 

    dc.full_name             AS customer_name, 

    dc.state_code, 

    dc.state_name, 

    dc.region, 

    dc.credit_band, 

    dc.customer_segment, 

    -- Product 

    dp.product_line, 

    dp.lob_code, 

    dp.lob_description, 

    dp.coverage_type, 

    -- Policy premium context 

    fp.annual_premium        AS policy_annual_premium, 

    fp.earned_premium_amount, 

    -- Incident date 

    dd_inc.calendar_date     AS incident_date, 

    dd_inc.year_number       AS incident_year, 

    dd_inc.month_name        AS incident_month, 

    dd_inc.quarter_name      AS incident_quarter, 

    dd_inc.fiscal_year       AS incident_fiscal_year, 

    -- Reported date 

    dd_rep.calendar_date     AS reported_date, 

    -- Closed date 

    dd_cls.calendar_date     AS closed_date 

FROM fact_claims fc 

INNER JOIN dim_customer dc  ON dc.customer_key      = fc.customer_key 

INNER JOIN fact_policy  fp  ON fp.policy_id         = fc.policy_id 

INNER JOIN dim_product  dp  ON dp.product_key       = fp.product_key 

INNER JOIN dim_date  dd_inc ON dd_inc.date_key      = fc.incident_date_key 

INNER JOIN dim_date  dd_rep ON dd_rep.date_key      = fc.reported_date_key 

LEFT  JOIN dim_date  dd_cls ON dd_cls.date_key      = fc.closed_date_key 

GO 

  

-- View: Executive KPI summary (used in dashboards) 

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'v_exec_kpi_summary' AND type = 'V') 

    DROP VIEW v_exec_kpi_summary 

GO 

  

CREATE VIEW v_exec_kpi_summary AS 

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

GO 

  

PRINT 'Aggregate marts and reporting views created successfully' 

GO 

  
