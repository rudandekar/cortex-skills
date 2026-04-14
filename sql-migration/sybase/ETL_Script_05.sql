-- =============================================================================
-- ETL SCRIPT 05 (ENHANCED): Aggregates, Views, Running Totals, Triggers
-- Description : Monthly aggregate marts, correlated-subquery rolling windows
--               (no window functions in Sybase IQ), gap-fill WHILE loop,
--               and INSTEAD OF trigger on reporting view.
-- Complexity  : COMPLEX — Correlated subqueries, INSTEAD OF trigger, WHILE gap-fill
-- =============================================================================

-- =============================================================================
-- SECTION 1: Policy Monthly Aggregate
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'agg_policy_monthly' AND type = 'U')
    DROP TABLE agg_policy_monthly
GO

CREATE TABLE agg_policy_monthly (
    agg_key             INT             IDENTITY NOT NULL,
    year_number         SMALLINT        NOT NULL,
    quarter_number      TINYINT         NOT NULL,
    month_number        TINYINT         NOT NULL,
    product_line        VARCHAR(50)     NOT NULL,
    lob_code            VARCHAR(10)     NOT NULL,
    state_code          CHAR(2)         NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    channel_code        VARCHAR(20)     NULL,
    policy_count        INT             NULL,
    new_policy_count    INT             NULL,
    renewal_count       INT             NULL,
    cancellation_count  INT             NULL,
    total_annual_premium    DECIMAL(18,2) NULL,
    total_earned_premium    DECIMAL(18,2) NULL,
    total_coverage_amount   DECIMAL(18,2) NULL,
    avg_premium         DECIMAL(18,2)   NULL,
    running_12m_premium DECIMAL(18,2)   NULL,   -- Populated in step 2
    yoy_premium_chg_pct DECIMAL(10,4)   NULL,   -- Populated in step 3
    PRIMARY KEY (agg_key)
) IN DBASPACE
GO

INSERT INTO agg_policy_monthly (
    year_number, quarter_number, month_number,
    product_line, lob_code, state_code, region, channel_code,
    policy_count, new_policy_count, renewal_count, cancellation_count,
    total_annual_premium, total_earned_premium, total_coverage_amount, avg_premium
)
SELECT
    dd.year_number,
    dd.quarter_number,
    dd.month_number,
    ISNULL(dp.product_line, 'UNKNOWN'),
    ISNULL(dp.lob_code, 'UNKNOWN'),
    ISNULL(dc.state_code, 'XX'),
    ISNULL(dc.region, 'UNKNOWN'),
    ISNULL(da.channel_code, 'UNKNOWN'),
    COUNT(DISTINCT fp.policy_id),
    COUNT(DISTINCT CASE WHEN fp.renewal_count = 0 THEN fp.policy_id ELSE NULL END),
    COUNT(DISTINCT CASE WHEN fp.renewal_count > 0
                             AND fp.status_code = 'RN' THEN fp.policy_id ELSE NULL END),
    COUNT(DISTINCT CASE WHEN fp.status_code = 'CN' THEN fp.policy_id ELSE NULL END),
    ROUND(SUM(ISNULL(fp.annual_premium, 0)), 2),
    ROUND(SUM(ISNULL(fp.earned_premium_amount, 0)), 2),
    ROUND(SUM(ISNULL(fp.coverage_amount, 0)), 2),
    CASE WHEN COUNT(fp.policy_id) > 0
         THEN ROUND(SUM(ISNULL(fp.annual_premium, 0)) / COUNT(fp.policy_id), 2)
         ELSE NULL END
FROM fact_policy fp
INNER JOIN dim_date    dd ON dd.date_key     = fp.effective_date_key
INNER JOIN dim_product dp ON dp.product_key  = fp.product_key
INNER JOIN dim_customer dc ON dc.customer_key = fp.customer_key
INNER JOIN dim_agent   da ON da.agent_key    = fp.agent_key
WHERE fp.status_code IN ('AC', 'RN', 'CN', 'EX')
  AND dd.year_number >= 2015
GROUP BY
    dd.year_number, dd.quarter_number, dd.month_number,
    ISNULL(dp.product_line, 'UNKNOWN'),
    ISNULL(dp.lob_code, 'UNKNOWN'),
    ISNULL(dc.state_code, 'XX'),
    ISNULL(dc.region, 'UNKNOWN'),
    ISNULL(da.channel_code, 'UNKNOWN')
GO

-- =============================================================================
-- SECTION 2: Rolling 12-Month Premium using Correlated Subquery
-- Sybase IQ has no window functions (no SUM() OVER PARTITION BY).
-- This correlated subquery is the Sybase IQ pattern.
-- Migration: replace entirely with Snowflake window functions:
--   SUM(total_annual_premium) OVER (
--     PARTITION BY product_line, lob_code, state_code
--     ORDER BY year_number, month_number
--     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
--   )
-- =============================================================================

UPDATE agg_policy_monthly
SET running_12m_premium = (
    SELECT SUM(a2.total_annual_premium)
    FROM agg_policy_monthly a2
    WHERE a2.product_line = agg_policy_monthly.product_line
      AND a2.lob_code     = agg_policy_monthly.lob_code
      AND a2.state_code   = agg_policy_monthly.state_code
      -- Rolling 12-month window: current month back 11 months
      AND (a2.year_number * 100 + a2.month_number)
            BETWEEN (agg_policy_monthly.year_number * 100 + agg_policy_monthly.month_number) - 11
            AND     (agg_policy_monthly.year_number * 100 + agg_policy_monthly.month_number)
)
GO

-- =============================================================================
-- SECTION 3: Year-over-Year Premium Change
-- Also a correlated subquery (no LAG() in Sybase IQ)
-- Migration: LAG(total_annual_premium, 12) OVER (PARTITION BY ... ORDER BY ...)
-- =============================================================================

UPDATE agg_policy_monthly
SET yoy_premium_chg_pct = (
    SELECT
        CASE WHEN ISNULL(py.total_annual_premium, 0) > 0
             THEN ROUND(
                (agg_policy_monthly.total_annual_premium - py.total_annual_premium)
                / py.total_annual_premium * 100.0, 4)
             ELSE NULL END
    FROM agg_policy_monthly py
    WHERE py.product_line  = agg_policy_monthly.product_line
      AND py.lob_code      = agg_policy_monthly.lob_code
      AND py.state_code    = agg_policy_monthly.state_code
      AND py.year_number   = agg_policy_monthly.year_number  - 1
      AND py.month_number  = agg_policy_monthly.month_number
)
GO

-- =============================================================================
-- SECTION 4: Gap-Fill for Missing Months (WHILE loop)
-- Ensures every product/lob/state combination has a row for every month
-- Migration: Snowflake GENERATOR() cross-joined to dimension values
-- =============================================================================

DECLARE @gap_year       SMALLINT
DECLARE @gap_month      TINYINT
DECLARE @gap_min_year   SMALLINT
DECLARE @gap_max_year   SMALLINT
DECLARE @gaps_inserted  INT

SELECT @gap_min_year = MIN(year_number),
       @gap_max_year = MAX(year_number)
FROM agg_policy_monthly

SELECT @gap_year  = @gap_min_year
SELECT @gap_month = 1
SELECT @gaps_inserted = 0

WHILE @gap_year <= @gap_max_year
BEGIN
    -- Insert zero-row for any product/lob/state/month combination that is missing
    INSERT INTO agg_policy_monthly (
        year_number, quarter_number, month_number,
        product_line, lob_code, state_code, region, channel_code,
        policy_count, new_policy_count, renewal_count, cancellation_count,
        total_annual_premium, total_earned_premium, total_coverage_amount, avg_premium,
        running_12m_premium, yoy_premium_chg_pct
    )
    SELECT
        @gap_year,
        CASE
            WHEN @gap_month BETWEEN 1 AND 3  THEN 1
            WHEN @gap_month BETWEEN 4 AND 6  THEN 2
            WHEN @gap_month BETWEEN 7 AND 9  THEN 3
            ELSE 4
        END,
        @gap_month,
        combos.product_line,
        combos.lob_code,
        combos.state_code,
        combos.region,
        combos.channel_code,
        0, 0, 0, 0,
        0.00, 0.00, 0.00, NULL,
        NULL, NULL
    FROM (
        -- All known product/lob/state/channel combinations
        SELECT DISTINCT product_line, lob_code, state_code, region, channel_code
        FROM agg_policy_monthly
    ) combos
    WHERE NOT EXISTS (
        SELECT 1 FROM agg_policy_monthly a
        WHERE a.year_number   = @gap_year
          AND a.month_number  = @gap_month
          AND a.product_line  = combos.product_line
          AND a.lob_code      = combos.lob_code
          AND a.state_code    = combos.state_code
    )

    SELECT @gaps_inserted = @gaps_inserted + @@ROWCOUNT

    -- Increment month
    IF @gap_month = 12
    BEGIN
        SELECT @gap_month = 1
        SELECT @gap_year  = @gap_year + 1
    END
    ELSE
        SELECT @gap_month = @gap_month + 1
END

PRINT 'Gap-fill complete: ' + CONVERT(VARCHAR, @gaps_inserted) + ' zero-rows inserted'
GO

-- =============================================================================
-- SECTION 5: Claims Aggregate with COMPUTE BY multi-level subtotals
-- COMPUTE BY at multiple levels — migration: GROUP BY ROLLUP() in Snowflake
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'agg_claims_monthly' AND type = 'U')
    DROP TABLE agg_claims_monthly
GO

CREATE TABLE agg_claims_monthly (
    agg_key             INT             IDENTITY NOT NULL,
    year_number         SMALLINT        NOT NULL,
    quarter_number      TINYINT         NOT NULL,
    month_number        TINYINT         NOT NULL,
    product_line        VARCHAR(50)     NOT NULL,
    lob_code            VARCHAR(10)     NOT NULL,
    state_code          CHAR(2)         NOT NULL,
    claim_type_code     VARCHAR(20)     NOT NULL,
    status_code         VARCHAR(10)     NOT NULL,
    claim_count         INT             NULL,
    open_claim_count    INT             NULL,
    closed_claim_count  INT             NULL,
    litigated_count     INT             NULL,
    cat_claim_count     INT             NULL,
    total_incurred      DECIMAL(18,2)   NULL,
    total_paid          DECIMAL(18,2)   NULL,
    total_reserved      DECIMAL(18,2)   NULL,
    total_outstanding   DECIMAL(18,2)   NULL,
    total_salvage       DECIMAL(18,2)   NULL,
    total_subrogation   DECIMAL(18,2)   NULL,
    avg_incurred        DECIMAL(18,2)   NULL,
    avg_settlement_days DECIMAL(10,2)   NULL,
    avg_report_lag_days DECIMAL(10,2)   NULL,
    earned_premium_ref  DECIMAL(18,2)   NULL,
    loss_ratio_pct      DECIMAL(10,4)   NULL,
    PRIMARY KEY (agg_key)
) IN DBASPACE
GO

INSERT INTO agg_claims_monthly (
    year_number, quarter_number, month_number,
    product_line, lob_code, state_code, claim_type_code, status_code,
    claim_count, open_claim_count, closed_claim_count, litigated_count, cat_claim_count,
    total_incurred, total_paid, total_reserved, total_outstanding,
    total_salvage, total_subrogation,
    avg_incurred, avg_settlement_days, avg_report_lag_days
)
SELECT
    dd.year_number, dd.quarter_number, dd.month_number,
    ISNULL(dp.product_line, 'UNKNOWN'),
    ISNULL(dp.lob_code, 'UNKNOWN'),
    ISNULL(dc.state_code, 'XX'),
    fc.claim_type_code,
    fc.status_code,
    COUNT(fc.claim_id),
    COUNT(CASE WHEN fc.status_code NOT IN ('CL','WD') THEN 1 END),
    COUNT(CASE WHEN fc.status_code IN ('CL','WD') THEN 1 END),
    COUNT(CASE WHEN fc.litigation_flag = 'Y' THEN 1 END),
    COUNT(CASE WHEN fc.catastrophe_code IS NOT NULL THEN 1 END),
    ROUND(SUM(ISNULL(fc.total_incurred, 0)), 2),
    ROUND(SUM(ISNULL(fc.total_paid, 0)), 2),
    ROUND(SUM(ISNULL(fc.total_reserved, 0)), 2),
    ROUND(SUM(ISNULL(fc.outstanding_reserve, 0)), 2),
    ROUND(SUM(ISNULL(fc.salvage_amount, 0)), 2),
    ROUND(SUM(ISNULL(fc.subrogation_amount, 0)), 2),
    ROUND(AVG(CAST(ISNULL(fc.total_incurred, 0) AS DECIMAL(18,4))), 2),
    ROUND(AVG(CAST(ISNULL(fc.claim_settlement_days, 0) AS DECIMAL(10,2))), 2),
    ROUND(AVG(CAST(ISNULL(fc.claim_report_lag_days, 0) AS DECIMAL(10,2))), 2)
FROM fact_claims fc
INNER JOIN dim_date     dd ON dd.date_key     = fc.incident_date_key
INNER JOIN fact_policy  fp ON fp.policy_id    = fc.policy_id
INNER JOIN dim_product  dp ON dp.product_key  = fp.product_key
INNER JOIN dim_customer dc ON dc.customer_key = fc.customer_key
WHERE dd.year_number >= 2015
GROUP BY
    dd.year_number, dd.quarter_number, dd.month_number,
    ISNULL(dp.product_line, 'UNKNOWN'),
    ISNULL(dp.lob_code, 'UNKNOWN'),
    ISNULL(dc.state_code, 'XX'),
    fc.claim_type_code,
    fc.status_code
GO

-- COMPUTE BY: nested subtotals — year > product_line > lob_code
-- Migration: GROUP BY ROLLUP(year_number, product_line, lob_code) in Snowflake
SELECT
    year_number, product_line, lob_code,
    SUM(claim_count)    AS claims,
    SUM(total_incurred) AS incurred,
    SUM(total_paid)     AS paid
FROM agg_claims_monthly
WHERE year_number >= 2020
GROUP BY year_number, product_line, lob_code
ORDER BY year_number, product_line, lob_code
COMPUTE SUM(SUM(claim_count)), SUM(SUM(total_incurred)) BY year_number, product_line
COMPUTE SUM(SUM(claim_count)), SUM(SUM(total_incurred)) BY year_number
GO

-- Update loss ratio
UPDATE agg_claims_monthly
SET
    earned_premium_ref = apm.total_earned_premium,
    loss_ratio_pct = CASE WHEN apm.total_earned_premium > 0
        THEN ROUND(agg_claims_monthly.total_incurred / apm.total_earned_premium * 100.0, 4)
        ELSE NULL END
FROM agg_claims_monthly acm
INNER JOIN agg_policy_monthly apm
    ON apm.year_number   = acm.year_number
   AND apm.month_number  = acm.month_number
   AND apm.lob_code      = acm.lob_code
   AND apm.state_code    = acm.state_code
GO

-- =============================================================================
-- SECTION 6: Reporting Views + INSTEAD OF Trigger
-- =============================================================================

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
    apm.state_code,
    SUM(apm.policy_count)                AS total_policies,
    SUM(apm.new_policy_count)            AS new_policies,
    SUM(apm.renewal_count)               AS renewals,
    SUM(apm.cancellation_count)          AS cancellations,
    SUM(apm.total_annual_premium)        AS gwp,
    SUM(apm.total_earned_premium)        AS earned_premium,
    MAX(apm.running_12m_premium)         AS rolling_12m_gwp,
    SUM(ISNULL(acm.claim_count, 0))      AS claim_count,
    SUM(ISNULL(acm.total_incurred, 0))   AS total_losses,
    SUM(ISNULL(acm.total_outstanding,0)) AS ibnr_reserve,
    CASE WHEN SUM(apm.total_earned_premium) > 0
         THEN ROUND(SUM(ISNULL(acm.total_incurred,0))
                    / SUM(apm.total_earned_premium) * 100, 2)
         ELSE NULL END                   AS loss_ratio_pct,
    CASE WHEN SUM(apm.policy_count) > 0
         THEN ROUND(SUM(ISNULL(acm.claim_count,0)) * 1000.0
                    / SUM(apm.policy_count), 4)
         ELSE NULL END                   AS claim_freq_per_1000,
    CASE WHEN SUM(ISNULL(acm.claim_count,0)) > 0
         THEN ROUND(SUM(ISNULL(acm.total_incurred,0))
                    / SUM(acm.claim_count), 2)
         ELSE NULL END                   AS avg_severity,
    AVG(apm.yoy_premium_chg_pct)         AS avg_yoy_premium_growth
FROM agg_policy_monthly apm
LEFT JOIN agg_claims_monthly acm
    ON acm.year_number   = apm.year_number
   AND acm.month_number  = apm.month_number
   AND acm.lob_code      = apm.lob_code
   AND acm.state_code    = apm.state_code
GROUP BY
    apm.year_number, apm.quarter_number, apm.month_number,
    apm.product_line, apm.lob_code, apm.region, apm.state_code
GO

-- INSTEAD OF trigger on v_exec_kpi_summary to intercept update attempts
-- INSTEAD OF triggers — no direct equivalent in Snowflake (views are not updatable)
-- Migration: remove entirely; enforce at application layer
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'trg_kpi_view_readonly' AND type = 'TR')
    DROP TRIGGER trg_kpi_view_readonly
GO

CREATE TRIGGER trg_kpi_view_readonly
ON v_exec_kpi_summary
INSTEAD OF UPDATE, DELETE
AS
BEGIN
    RAISERROR (
        'v_exec_kpi_summary is read-only. Update the underlying aggregate tables directly.',
        16, 1
    )
    -- Roll back any accidental DML
    ROLLBACK TRANSACTION
END
GO

-- Standard trigger on agg_policy_monthly: maintain audit trail on updates
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'trg_agg_policy_audit' AND type = 'TR')
    DROP TRIGGER trg_agg_policy_audit
GO

CREATE TRIGGER trg_agg_policy_audit
ON agg_policy_monthly
AFTER UPDATE
AS
BEGIN
    -- Log aggregate updates for lineage tracking
    INSERT INTO etl_audit_log (
        script_name, object_name, check_type, status, row_count, error_message
    )
    SELECT
        'TRIGGER', 'agg_policy_monthly', 'AGG_UPDATE',
        'PASS', COUNT(*),
        'Aggregate updated by: ' + USER_NAME()
    FROM inserted
END
GO

PRINT 'Script 05 complete: Aggregates, views, and triggers created'
GO


