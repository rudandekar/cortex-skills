-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql3.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L3_fact (DML)
-- [META] Objects:       fact_policy, fact_claims, fact_payments
-- =============================================================================


-- ============================================================
-- SECTION 1: Populate FACT_POLICY
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] DATEDIFF preserved (Snowflake native)
-- [CONVERT] ROUND/CAST preserved (Snowflake native)
-- ============================================================

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
    COALESCE(dc.customer_key,  -1),                                          -- [CONVERT] ISNULL → COALESCE; -1 = 'Unknown' member
    -- Agent dimension key (current record)
    COALESCE(da.agent_key,     -1),
    -- Product dimension key
    COALESCE(dp.product_key,   -1),
    -- Date keys (YYYYMMDD integer)
    TO_NUMBER(TO_CHAR(sp.effective_date, 'YYYYMMDD')),                       -- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), ..., 112))
    TO_NUMBER(TO_CHAR(sp.expiry_date,    'YYYYMMDD')),                       -- [CONVERT] same pattern
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
    ),
    1001   -- batch_id placeholder
FROM stg_policies sp
LEFT OUTER JOIN dim_customer dc
    ON  dc.customer_id      = sp.customer_id
    AND dc.scd_current_flag = 'Y'
LEFT OUTER JOIN dim_agent da
    ON  da.agent_id         = sp.agent_id
    AND da.scd_current_flag = 'Y'
LEFT OUTER JOIN dim_product dp
    ON  dp.product_code     = sp.product_code
-- Avoid duplicates: only insert if not already loaded
WHERE NOT EXISTS (
    SELECT 1 FROM fact_policy fp
    WHERE  fp.policy_id = sp.policy_id
);

-- Update existing fact rows where staging has newer data
-- [CONVERT] UPDATE ... FROM (Sybase) → UPDATE ... FROM (Snowflake syntax)
UPDATE fact_policy AS fp
SET    fp.status_code             = sp.status_code,
       fp.annual_premium          = sp.annual_premium,
       fp.coverage_amount         = sp.coverage_amount,
       fp.earned_premium_amount   = ROUND(
                                        sp.annual_premium *
                                        (CAST(DATEDIFF(DAY, sp.effective_date, sp.expiry_date)
                                         AS DECIMAL(10,4)) / 365.0), 2),
       fp.dw_insert_ts            = CURRENT_TIMESTAMP()                      -- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
FROM   stg_policies sp
WHERE  fp.policy_id = sp.policy_id
AND (  fp.status_code     <> sp.status_code
    OR fp.annual_premium  <> COALESCE(sp.annual_premium, fp.annual_premium)
);


-- ============================================================
-- SECTION 2: Populate FACT_CLAIMS
-- [CONVERT] Same date-key pattern, ISNULL→COALESCE, DATEDIFF native
-- ============================================================

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
    COALESCE(dc.customer_key, -1),
    COALESCE(dp.product_key,  -1),
    TO_NUMBER(TO_CHAR(sc.incident_date,  'YYYYMMDD')),
    TO_NUMBER(TO_CHAR(sc.reported_date,  'YYYYMMDD')),
    CASE WHEN sc.closed_date IS NOT NULL
         THEN TO_NUMBER(TO_CHAR(sc.closed_date, 'YYYYMMDD'))
         ELSE NULL END,
    sc.claim_number,
    sc.claim_type_code,
    sc.status_code,
    sc.fault_indicator,
    sc.litigation_flag,
    COALESCE(sc.total_incurred, 0),
    COALESCE(sc.total_paid,     0),
    COALESCE(sc.total_reserved, 0),
    -- Outstanding = reserved minus paid
    COALESCE(sc.total_reserved, 0) - COALESCE(sc.total_paid, 0),
    -- Lag from incident to report (in days)
    DATEDIFF(DAY, sc.incident_date, sc.reported_date),
    -- Settlement days: null if still open
    CASE WHEN sc.closed_date IS NOT NULL
         THEN DATEDIFF(DAY, sc.reported_date, sc.closed_date)
         ELSE NULL END,
    -- Loss ratio against policy earned premium
    CASE WHEN COALESCE(fp.earned_premium_amount, 0) > 0
         THEN ROUND(COALESCE(sc.total_incurred, 0) / fp.earned_premium_amount * 100.0, 4)
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
);

-- Refresh measures for existing open claims (amounts change as payments made)
-- [CONVERT] UPDATE ... FROM → Snowflake UPDATE ... FROM
UPDATE fact_claims AS fc
SET    fc.status_code           = sc.status_code,
       fc.total_incurred        = COALESCE(sc.total_incurred, 0),
       fc.total_paid            = COALESCE(sc.total_paid,     0),
       fc.total_reserved        = COALESCE(sc.total_reserved, 0),
       fc.outstanding_reserve   = COALESCE(sc.total_reserved, 0) - COALESCE(sc.total_paid, 0),
       fc.closed_date_key       = CASE WHEN sc.closed_date IS NOT NULL
                                       THEN TO_NUMBER(TO_CHAR(sc.closed_date, 'YYYYMMDD'))
                                       ELSE NULL END,
       fc.claim_settlement_days = CASE WHEN sc.closed_date IS NOT NULL
                                       THEN DATEDIFF(DAY, sc.reported_date, sc.closed_date)
                                       ELSE NULL END,
       fc.litigation_flag       = sc.litigation_flag,
       fc.dw_insert_ts          = CURRENT_TIMESTAMP()
FROM   stg_claims sc
WHERE  fc.claim_id = sc.claim_id
AND (  fc.status_code       <> sc.status_code
    OR fc.total_paid         < COALESCE(sc.total_paid,     0)
    OR fc.total_reserved    <> COALESCE(sc.total_reserved, 0)
);


-- ============================================================
-- SECTION 3: Populate FACT_PAYMENTS
-- ============================================================

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
    COALESCE(dc.customer_key, -1),
    COALESCE(fp.product_key,  -1),
    TO_NUMBER(TO_CHAR(sp.payment_date, 'YYYYMMDD')),
    CASE WHEN sp.due_date IS NOT NULL
         THEN TO_NUMBER(TO_CHAR(sp.due_date, 'YYYYMMDD'))
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
);

-- ============================================================
-- Post-load row count validation
-- [REMOVE] GO separators removed
-- [REMOVE] PRINT statement removed
-- ============================================================

SELECT 'fact_policy'   AS fact_table, COUNT(*) AS total_rows,
       SUM(annual_premium)  AS sum_premium
FROM   fact_policy
UNION ALL
SELECT 'fact_claims',   COUNT(*), SUM(total_incurred)
FROM   fact_claims
UNION ALL
SELECT 'fact_payments', COUNT(*), SUM(payment_amount)
FROM   fact_payments;

-- Referential integrity check: unresolved dimension keys
SELECT 'Unresolved customer keys' AS check_name, COUNT(*) AS cnt
FROM   fact_policy WHERE customer_key = -1
UNION ALL
SELECT 'Unresolved agent keys',    COUNT(*) FROM fact_policy WHERE agent_key   = -1
UNION ALL
SELECT 'Unresolved product keys',  COUNT(*) FROM fact_policy WHERE product_key = -1
UNION ALL
SELECT 'Claims no customer key',   COUNT(*) FROM fact_claims WHERE customer_key = -1;
