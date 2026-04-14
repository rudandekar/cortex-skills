-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql5.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L6_presentation (DDL)
-- [META] Objects:       v_policy_detail, v_claims_analysis, v_exec_kpi_summary
-- =============================================================================

-- ============================================================
-- View: Full policy detail with all dimension attributes resolved
-- [CONVERT] IF EXISTS(sysobjects)/DROP VIEW/GO → CREATE OR REPLACE VIEW
-- ============================================================

CREATE OR REPLACE VIEW v_policy_detail AS
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
INNER JOIN dim_date  dd_exp ON dd_exp.date_key  = fp.expiry_date_key;

-- ============================================================
-- View: Claims with full context for loss analysis
-- ============================================================

CREATE OR REPLACE VIEW v_claims_analysis AS
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
LEFT  JOIN dim_date  dd_cls ON dd_cls.date_key      = fc.closed_date_key;

-- ============================================================
-- View: Executive KPI summary (used in dashboards)
-- ============================================================

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
    apm.product_line, apm.lob_code, apm.region;

-- [REMOVE] PRINT 'Aggregate marts and reporting views created successfully'
