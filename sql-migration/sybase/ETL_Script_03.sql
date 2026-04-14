-- =============================================================================
-- ETL SCRIPT 03 (ENHANCED): Load Fact Tables
-- Description : policy, claims, payments, reinsurance facts with surrogate
--               key resolution, INSERT...EXEC patterns, temp table staging,
--               and full @@ERROR / transaction handling.
-- Complexity  : COMPLEX — INSERT...EXEC, multi-step temp tables, @@TRANCOUNT
-- =============================================================================

-- =============================================================================
-- SECTION 1: Fact Policy
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_policy' AND type = 'U')
BEGIN
    CREATE TABLE fact_policy (
        policy_key              INT             IDENTITY NOT NULL,
        policy_id               BIGINT          NOT NULL,
        policy_number           VARCHAR(30)     NOT NULL,
        customer_key            INT             NOT NULL,
        agent_key               INT             NOT NULL,
        product_key             INT             NOT NULL,
        territory_key           INT             NULL,
        underwriter_key         INT             NULL,
        effective_date_key      INT             NOT NULL,
        expiry_date_key         INT             NOT NULL,
        status_code             CHAR(2)         NOT NULL,
        annual_premium          DECIMAL(18,2)   NULL,
        coverage_amount         DECIMAL(18,2)   NULL,
        earned_premium_amount   DECIMAL(18,2)   NULL,
        policy_term_days        INT             NULL,
        renewal_count           TINYINT         NULL,
        cancellation_reason     VARCHAR(50)     NULL,
        is_reinsured            CHAR(1)         NULL DEFAULT 'N',
        reinsurance_premium_ceded DECIMAL(18,2) NULL,
        dw_created_ts           DATETIME        NOT NULL DEFAULT GETDATE(),
        dw_updated_ts           DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (policy_key)
    ) IN DBASPACE
END
GO

-- Step 1: Resolve all surrogate keys into a temp table
-- This avoids repeated lookup joins in the final INSERT
CREATE TABLE #fact_policy_stage (
    policy_id               BIGINT          NOT NULL,
    policy_number           VARCHAR(30)     NOT NULL,
    customer_key            INT             NULL,
    agent_key               INT             NULL,
    product_key             INT             NULL,
    territory_key           INT             NULL,
    underwriter_key         INT             NULL,
    effective_date_key      INT             NULL,
    expiry_date_key         INT             NULL,
    status_code             CHAR(2)         NOT NULL,
    annual_premium          DECIMAL(18,2)   NULL,
    coverage_amount         DECIMAL(18,2)   NULL,
    earned_premium_amount   DECIMAL(18,2)   NULL,
    policy_term_days        INT             NULL,
    renewal_count           TINYINT         NULL,
    cancellation_reason     VARCHAR(50)     NULL,
    key_resolve_status      VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
)
GO

-- Populate staging with surrogate key lookups
INSERT INTO #fact_policy_stage (
    policy_id, policy_number, status_code, annual_premium, coverage_amount,
    earned_premium_amount, policy_term_days, renewal_count, cancellation_reason,
    customer_key, agent_key, product_key, territory_key,
    effective_date_key, expiry_date_key
)
SELECT
    p.policy_id,
    p.policy_number,
    p.status_code,
    p.annual_premium,
    p.coverage_amount,
    -- Earned premium: prorated by days elapsed vs total term
    CASE
        WHEN DATEDIFF(day, p.effective_date, p.expiry_date) > 0
        THEN ROUND(
            p.annual_premium
            * CAST(
                CASE WHEN GETDATE() >= p.expiry_date
                     THEN DATEDIFF(day, p.effective_date, p.expiry_date)
                     ELSE DATEDIFF(day, p.effective_date, GETDATE())
                END AS DECIMAL(10,4))
            / DATEDIFF(day, p.effective_date, p.expiry_date),
            2)
        ELSE p.annual_premium
    END,
    DATEDIFF(day, p.effective_date, p.expiry_date),
    p.renewal_count,
    p.cancellation_reason,
    -- Customer surrogate (SCD2: get current record)
    dc.customer_key,
    -- Agent surrogate (SCD2: get current record)
    da.agent_key,
    -- Product surrogate
    dp.product_key,
    -- Territory surrogate
    dt.territory_key,
    -- Date surrogates from date dimension
    dd_eff.date_key,
    dd_exp.date_key
FROM stg_policies p
LEFT JOIN dim_customer dc
    ON dc.customer_id = p.customer_id AND dc.is_current = 'Y'
LEFT JOIN dim_agent da
    ON da.agent_id    = p.agent_id    AND da.is_current = 'Y'
LEFT JOIN dim_product dp
    ON dp.product_code = p.product_code
LEFT JOIN dim_territory dt
    ON dt.territory_code = p.territory_code AND dt.active_flag = 'Y'
LEFT JOIN dim_date dd_eff
    ON dd_eff.calendar_date = p.effective_date
LEFT JOIN dim_date dd_exp
    ON dd_exp.calendar_date = p.expiry_date
GO

-- Validate key resolution — flag records with unresolved surrogates
UPDATE #fact_policy_stage
SET key_resolve_status = 'MISSING_KEYS'
WHERE customer_key IS NULL
   OR agent_key    IS NULL
   OR product_key  IS NULL
   OR effective_date_key IS NULL
   OR expiry_date_key    IS NULL
GO

DECLARE @unresolved INT
SELECT  @unresolved = COUNT(*) FROM #fact_policy_stage WHERE key_resolve_status = 'MISSING_KEYS'

IF @unresolved > 0
BEGIN
    -- Log to audit but do not abort — use default/unknown dimension members
    INSERT INTO etl_audit_log (
        script_name, object_name, check_type, status, row_count, error_message, run_ts
    )
    VALUES (
        'etl_03_load_facts', 'fact_policy', 'SURROGATE_KEY_RESOLVE',
        'WARNING', @unresolved,
        CONVERT(VARCHAR, @unresolved) + ' policies had unresolved surrogate keys - using defaults',
        GETDATE()
    )

    -- Assign default dimension key (-1 = Unknown member, must pre-exist in dims)
    UPDATE #fact_policy_stage
    SET
        customer_key        = ISNULL(customer_key, -1),
        agent_key           = ISNULL(agent_key, -1),
        product_key         = ISNULL(product_key, -1),
        effective_date_key  = ISNULL(effective_date_key, 19000101),
        expiry_date_key     = ISNULL(expiry_date_key, 99991231),
        key_resolve_status  = 'DEFAULTED'
    WHERE key_resolve_status = 'MISSING_KEYS'
END
GO

-- Step 2: UPDATE existing policies (status, earned premium, amounts change)
BEGIN TRAN update_fact_policy

    UPDATE fact_policy
    SET
        status_code             = s.status_code,
        annual_premium          = s.annual_premium,
        coverage_amount         = s.coverage_amount,
        earned_premium_amount   = s.earned_premium_amount,
        cancellation_reason     = s.cancellation_reason,
        renewal_count           = s.renewal_count,
        customer_key            = s.customer_key,
        agent_key               = s.agent_key,
        dw_updated_ts           = GETDATE()
    FROM fact_policy fp
    INNER JOIN #fact_policy_stage s ON s.policy_id = fp.policy_id

    IF @@ERROR <> 0
    BEGIN
        ROLLBACK TRAN update_fact_policy
        RAISERROR ('FATAL: fact_policy UPDATE failed. Transaction rolled back.', 16, 1)
        DROP TABLE #fact_policy_stage
        RETURN
    END

COMMIT TRAN update_fact_policy
PRINT 'fact_policy updated: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' rows'
GO

-- Step 3: INSERT new policies
BEGIN TRAN insert_fact_policy

    INSERT INTO fact_policy (
        policy_id, policy_number,
        customer_key, agent_key, product_key, territory_key, underwriter_key,
        effective_date_key, expiry_date_key,
        status_code, annual_premium, coverage_amount, earned_premium_amount,
        policy_term_days, renewal_count, cancellation_reason
    )
    SELECT
        s.policy_id, s.policy_number,
        s.customer_key, s.agent_key, s.product_key, s.territory_key, s.underwriter_key,
        s.effective_date_key, s.expiry_date_key,
        s.status_code, s.annual_premium, s.coverage_amount, s.earned_premium_amount,
        s.policy_term_days, s.renewal_count, s.cancellation_reason
    FROM #fact_policy_stage s
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_policy fp WHERE fp.policy_id = s.policy_id
    )

    IF @@ERROR <> 0
    BEGIN
        ROLLBACK TRAN insert_fact_policy
        RAISERROR ('FATAL: fact_policy INSERT failed. Transaction rolled back.', 16, 1)
        DROP TABLE #fact_policy_stage
        RETURN
    END

    PRINT 'fact_policy inserted: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' rows'

COMMIT TRAN insert_fact_policy

DROP TABLE #fact_policy_stage
GO

-- =============================================================================
-- SECTION 2: Fact Claims
-- Uses INSERT...EXEC to pull pre-validated records from a validation procedure
-- INSERT...EXEC is Sybase-specific — no direct Snowflake equivalent
-- Migration: replace with CTE or temp table populated directly
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_claims' AND type = 'U')
BEGIN
    CREATE TABLE fact_claims (
        claim_key               INT             IDENTITY NOT NULL,
        claim_id                BIGINT          NOT NULL,
        claim_number            VARCHAR(30)     NOT NULL,
        policy_id               BIGINT          NOT NULL,
        customer_key            INT             NOT NULL,
        product_key             INT             NOT NULL,
        incident_date_key       INT             NOT NULL,
        reported_date_key       INT             NOT NULL,
        closed_date_key         INT             NULL,
        claim_type_code         VARCHAR(20)     NOT NULL,
        sub_type_code           VARCHAR(20)     NULL,
        status_code             VARCHAR(10)     NOT NULL,
        fault_indicator         CHAR(1)         NULL,
        litigation_flag         CHAR(1)         NULL,
        catastrophe_code        VARCHAR(10)     NULL,
        reinsurance_flag        CHAR(1)         NULL,
        total_incurred          DECIMAL(18,2)   NULL,
        total_paid              DECIMAL(18,2)   NULL,
        total_reserved          DECIMAL(18,2)   NULL,
        salvage_amount          DECIMAL(18,2)   NULL,
        subrogation_amount      DECIMAL(18,2)   NULL,
        outstanding_reserve     DECIMAL(18,2)   NULL,
        claim_report_lag_days   INT             NULL,
        claim_settlement_days   INT             NULL,
        loss_ratio_pct          DECIMAL(10,4)   NULL,
        dw_created_ts           DATETIME        NOT NULL DEFAULT GETDATE(),
        dw_updated_ts           DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (claim_key)
    ) IN DBASPACE
END
GO

-- Temp table to receive validated claims from the validation procedure
-- INSERT...EXEC pattern: execute a proc and insert its result set into a table
CREATE TABLE #validated_claims (
    claim_id            BIGINT      NOT NULL,
    claim_number        VARCHAR(30) NOT NULL,
    policy_id           BIGINT      NOT NULL,
    customer_id         BIGINT      NOT NULL,
    incident_date       DATE        NOT NULL,
    reported_date       DATE        NOT NULL,
    closed_date         DATE        NULL,
    claim_type_code     VARCHAR(20) NOT NULL,
    sub_type_code       VARCHAR(20) NULL,
    status_code         VARCHAR(10) NOT NULL,
    fault_indicator     CHAR(1)     NULL,
    litigation_flag     CHAR(1)     NULL,
    catastrophe_code    VARCHAR(10) NULL,
    reinsurance_flag    CHAR(1)     NULL,
    total_incurred      DECIMAL(18,2) NULL,
    total_paid          DECIMAL(18,2) NULL,
    total_reserved      DECIMAL(18,2) NULL,
    salvage_amount      DECIMAL(18,2) NULL,
    subrogation_amount  DECIMAL(18,2) NULL,
    validation_status   VARCHAR(20) NOT NULL DEFAULT 'VALID'
)
GO

-- INSERT...EXEC: capture output of sp_validate_claims
-- sp_validate_claims returns rows with basic DQ applied
-- Migration to Snowflake: replace with direct SELECT from staging after CTE-based validation
INSERT INTO #validated_claims
EXEC sp_validate_claims @batch_date = NULL, @include_invalid = 0
GO

IF @@ERROR <> 0
BEGIN
    RAISERROR ('ERROR: sp_validate_claims execution failed. Claims load aborted.', 16, 1)
    DROP TABLE #validated_claims
    RETURN
END

DECLARE @claim_count INT
SELECT  @claim_count = COUNT(*) FROM #validated_claims
PRINT 'Validated claims received from sp_validate_claims: ' + CONVERT(VARCHAR, @claim_count)
GO

-- Update existing claims (status, reserves, paid amounts change frequently)
UPDATE fact_claims
SET
    status_code             = v.status_code,
    total_paid              = v.total_paid,
    total_reserved          = v.total_reserved,
    salvage_amount          = v.salvage_amount,
    subrogation_amount      = v.subrogation_amount,
    outstanding_reserve     = ISNULL(v.total_reserved, 0) - ISNULL(v.total_paid, 0),
    closed_date_key         = CASE
        WHEN v.closed_date IS NOT NULL
        THEN CONVERT(INT, CONVERT(VARCHAR(8), v.closed_date, 112))
        ELSE NULL
    END,
    claim_settlement_days   = CASE
        WHEN v.closed_date IS NOT NULL
        THEN DATEDIFF(day, v.reported_date, v.closed_date)
        ELSE NULL
    END,
    dw_updated_ts           = GETDATE()
FROM fact_claims fc
INNER JOIN #validated_claims v ON v.claim_id = fc.claim_id
GO

-- Insert new claims
INSERT INTO fact_claims (
    claim_id, claim_number, policy_id,
    customer_key, product_key,
    incident_date_key, reported_date_key, closed_date_key,
    claim_type_code, sub_type_code, status_code,
    fault_indicator, litigation_flag, catastrophe_code, reinsurance_flag,
    total_incurred, total_paid, total_reserved,
    salvage_amount, subrogation_amount, outstanding_reserve,
    claim_report_lag_days, claim_settlement_days
)
SELECT
    v.claim_id, v.claim_number, v.policy_id,
    ISNULL(dc.customer_key, -1),
    ISNULL(fp.product_key,  -1),
    ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), v.incident_date,  112)), 19000101),
    ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), v.reported_date,  112)), 19000101),
    CASE WHEN v.closed_date IS NOT NULL
         THEN CONVERT(INT, CONVERT(VARCHAR(8), v.closed_date, 112))
         ELSE NULL END,
    v.claim_type_code, v.sub_type_code, v.status_code,
    v.fault_indicator, v.litigation_flag, v.catastrophe_code, v.reinsurance_flag,
    v.total_incurred, v.total_paid, v.total_reserved,
    v.salvage_amount, v.subrogation_amount,
    ISNULL(v.total_reserved, 0) - ISNULL(v.total_paid, 0),
    DATEDIFF(day, v.incident_date, v.reported_date),
    CASE WHEN v.closed_date IS NOT NULL
         THEN DATEDIFF(day, v.reported_date, v.closed_date)
         ELSE NULL END
FROM #validated_claims v
LEFT JOIN dim_customer dc
    ON dc.customer_id = v.customer_id AND dc.is_current = 'Y'
LEFT JOIN fact_policy fp
    ON fp.policy_id   = v.policy_id
WHERE NOT EXISTS (
    SELECT 1 FROM fact_claims fc WHERE fc.claim_id = v.claim_id
)
GO

PRINT 'fact_claims inserted: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' rows'

-- Update loss ratio on newly inserted and updated claims
UPDATE fact_claims
SET loss_ratio_pct = CASE
    WHEN ISNULL(fp.annual_premium, 0) > 0
    THEN ROUND(ISNULL(fc.total_incurred, 0) / fp.annual_premium * 100.0, 4)
    ELSE NULL END
FROM fact_claims fc
INNER JOIN fact_policy fp ON fp.policy_id = fc.policy_id

IF @@ERROR <> 0
    PRINT 'WARNING: Loss ratio update encountered errors. Manual review required.'
GO

DROP TABLE #validated_claims
GO

-- =============================================================================
-- SECTION 3: Fact Reinsurance — New table
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'fact_reinsurance' AND type = 'U')
BEGIN
    CREATE TABLE fact_reinsurance (
        reinsurance_key         INT             IDENTITY NOT NULL,
        cession_id              BIGINT          NOT NULL,
        treaty_code             VARCHAR(20)     NOT NULL,
        treaty_type             VARCHAR(20)     NULL,
        policy_id               BIGINT          NOT NULL,
        claim_id                BIGINT          NULL,
        product_key             INT             NOT NULL,
        period_start_key        INT             NOT NULL,
        period_end_key          INT             NOT NULL,
        settlement_date_key     INT             NULL,
        reinsurer_code          VARCHAR(20)     NOT NULL,
        layer_number            TINYINT         NOT NULL,
        cedant_amount           DECIMAL(18,2)   NOT NULL,
        reinsurer_share_pct     DECIMAL(7,4)    NOT NULL,
        reinsurer_amount        DECIMAL(18,2)   NOT NULL,
        retention_amount        DECIMAL(18,2)   NULL,
        reinstatement_flag      CHAR(1)         NULL,
        cession_type            VARCHAR(10)     NULL,   -- PREMIUM or LOSS
        dw_created_ts           DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (reinsurance_key)
    ) IN DBASPACE
END
GO

INSERT INTO fact_reinsurance (
    cession_id, treaty_code, treaty_type,
    policy_id, claim_id, product_key,
    period_start_key, period_end_key, settlement_date_key,
    reinsurer_code, layer_number,
    cedant_amount, reinsurer_share_pct, reinsurer_amount,
    retention_amount, reinstatement_flag,
    cession_type
)
SELECT
    r.cession_id, r.treaty_code, r.treaty_type,
    r.policy_id, r.claim_id,
    ISNULL(dp.product_key, -1),
    ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), r.period_start, 112)), 19000101),
    ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), r.period_end,   112)), 19000101),
    CASE WHEN r.settlement_date IS NOT NULL
         THEN CONVERT(INT, CONVERT(VARCHAR(8), r.settlement_date, 112))
         ELSE NULL END,
    r.reinsurer_code, r.layer_number,
    r.cedant_amount, r.reinsurer_share_pct, r.reinsurer_amount,
    r.retention_amount, r.reinstatement_flag,
    CASE WHEN r.claim_id IS NULL THEN 'PREMIUM' ELSE 'LOSS' END
FROM stg_reinsurance r
LEFT JOIN fact_policy fp ON fp.policy_id = r.policy_id
LEFT JOIN dim_product  dp ON dp.product_key = fp.product_key
WHERE NOT EXISTS (
    SELECT 1 FROM fact_reinsurance fr WHERE fr.cession_id = r.cession_id
)
GO

PRINT 'fact_reinsurance inserted: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' rows'
GO

PRINT 'Script 03 complete: Facts loaded'
GO
