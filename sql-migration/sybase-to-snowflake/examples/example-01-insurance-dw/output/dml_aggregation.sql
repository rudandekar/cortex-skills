-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql5.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L5_aggregation (DDL + DML)
-- [META] Objects:       agg_policy_monthly, agg_claims_monthly
-- =============================================================================

-- ============================================================
-- SECTION 1: Monthly Policy Summary (aggregate mart)
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- ============================================================

CREATE OR REPLACE TABLE agg_policy_monthly (
    agg_key                 INT AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    total_annual_premium    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    total_earned_premium    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    total_coverage_amount   DECIMAL(18,2)   NOT NULL DEFAULT 0,
    avg_premium_per_policy  DECIMAL(18,2)   NULL,
    -- Renewal / retention metrics
    renewed_policy_count    INT             NOT NULL DEFAULT 0,
    -- Audit
    dw_insert_ts            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Populate monthly policy aggregates
-- [CONVERT] ISNULL() → COALESCE()
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
    COALESCE(dp.product_line, 'UNKNOWN'),                                     -- [CONVERT] ISNULL → COALESCE
    COALESCE(dp.lob_code,     'UNKNOWN'),
    COALESCE(da.channel_code, 'UNKNOWN'),
    COALESCE(dc.region,       'UNKNOWN'),
    COALESCE(dc.state_code,   'XX'),
    fp.status_code,
    COUNT(fp.policy_fact_key)                   AS policy_count,
    SUM(CASE WHEN dd_eff.year_number  = dd_eff.year_number
              AND dd_eff.month_number = dd_eff.month_number
             THEN 1 ELSE 0 END)                 AS new_policy_count,    -- simplified (preserves source logic)
    SUM(CASE WHEN fp.status_code = 'CN' THEN 1 ELSE 0 END) AS cancelled_count,
    SUM(COALESCE(fp.annual_premium,        0))    AS total_annual_premium,
    SUM(COALESCE(fp.earned_premium_amount, 0))    AS total_earned_premium,
    SUM(COALESCE(fp.coverage_amount,       0))    AS total_coverage_amount,
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
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code,     'UNKNOWN'),
    COALESCE(da.channel_code, 'UNKNOWN'),
    COALESCE(dc.region,       'UNKNOWN'),
    COALESCE(dc.state_code,   'XX'),
    fp.status_code;


-- ============================================================
-- SECTION 2: Monthly Claims Summary
-- ============================================================

CREATE OR REPLACE TABLE agg_claims_monthly (
    agg_key                     INT AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
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
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code,     'UNKNOWN'),
    fc.claim_type_code,
    COALESCE(dc.state_code,   'XX'),
    fc.status_code,
    COUNT(fc.claim_fact_key),
    SUM(CASE WHEN fc.status_code NOT IN ('CL','WD') THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.status_code = 'CL'             THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.litigation_flag = 'Y'          THEN 1 ELSE 0 END),
    SUM(CASE WHEN fc.fault_indicator = 'Y'          THEN 1 ELSE 0 END),
    SUM(COALESCE(fc.total_incurred,    0)),
    SUM(COALESCE(fc.total_paid,        0)),
    SUM(COALESCE(fc.total_reserved,    0)),
    SUM(COALESCE(fc.outstanding_reserve, 0)),
    CASE WHEN COUNT(fc.claim_fact_key) > 0
         THEN ROUND(SUM(COALESCE(fc.total_incurred, 0)) / COUNT(fc.claim_fact_key), 2)
         ELSE NULL END,
    ROUND(AVG(CAST(COALESCE(fc.claim_settlement_days, 0) AS DECIMAL(10,2))), 2),
    ROUND(AVG(CAST(COALESCE(fc.claim_report_lag_days, 0) AS DECIMAL(10,2))), 2)
FROM fact_claims fc
INNER JOIN dim_date     dd ON dd.date_key    = fc.incident_date_key
INNER JOIN fact_policy  fp ON fp.policy_id   = fc.policy_id
INNER JOIN dim_product  dp ON dp.product_key = fp.product_key
INNER JOIN dim_customer dc ON dc.customer_key = fc.customer_key
WHERE dd.year_number >= 2015
GROUP BY
    dd.year_number, dd.month_number, dd.quarter_number,
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code,     'UNKNOWN'),
    fc.claim_type_code,
    COALESCE(dc.state_code,   'XX'),
    fc.status_code;

-- Enrich with loss ratio by joining to policy aggregate
-- [CONVERT] Sybase UPDATE ... FROM syntax → Snowflake UPDATE ... FROM syntax
UPDATE agg_claims_monthly AS acm
SET    acm.earned_premium_ref = apm.total_earned_premium,
       acm.loss_ratio_pct     = CASE WHEN apm.total_earned_premium > 0
                                     THEN ROUND(acm.total_incurred
                                                / apm.total_earned_premium * 100.0, 4)
                                     ELSE NULL END
FROM   agg_policy_monthly apm
WHERE  apm.year_number    = acm.year_number
AND    apm.month_number   = acm.month_number
AND    apm.lob_code       = acm.lob_code
AND    apm.state_code     = acm.state_code;
