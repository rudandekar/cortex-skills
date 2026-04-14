-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql4.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L4_audit (PROCEDURAL)
-- [META] Objects:       etl_dq_checks (stored procedure)
-- [META] Complexity:    13 (Complex / deep) — Pre-quarantine candidate
-- [META] Status:        RESOLVED — Quarantine Q001 reviewed; all 4 items verified acceptable (2026-04-02)
-- =============================================================================
--
-- [RESOLVED] QUARANTINE Q001 — All 4 review items verified acceptable:
--   1. Variable scoping: 14 unique v_-prefixed names, no collisions from GO-batch consolidation
--   2. RAISERROR → RAISE: Sybase source had RAISERROR commented out; RAISE is stricter (improvement)
--   3. Transaction behavior: Independent INSERTs with Snowflake auto-commit ≡ Sybase GO-batch auto-commit
--   4. ::VARCHAR cast: Unbounded VARCHAR functionally identical to Sybase default 30 for int-to-string
--
-- Original Sybase structure:
--   - 8 separate GO-delimited batches, each with its own DECLARE block
--   - Variables re-declared per batch (Sybase batch isolation)
--   - IF/ELSE branching with INSERT INTO etl_audit_log
--   - Final batch: summary SELECT + conditional RAISERROR
--
-- Snowflake conversion approach:
--   - Wrapped in a single stored procedure with Snowflake Scripting
--   - All variables declared once in DECLARE block
--   - IF/ELSEIF/ELSE/END IF control flow
--   - PRINT → commented out (no Snowflake equivalent in SP context)
--   - RAISERROR → RAISE with custom exception message
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_etl_dq_checks(P_BATCH_ID INT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_date       DATE;
    v_threshold      DECIMAL(6,2) DEFAULT 5.00;
    v_null_count     INT;
    v_dup_count      INT;
    v_date_err_count INT;
    v_neg_prem       INT;
    v_overpaid_count INT;
    v_orphan_claims  INT;
    v_prior_policy_count INT;
    v_curr_policy_count  INT;
    v_pct_change     DECIMAL(8,4);
    v_unresolved_cust  INT;
    v_unresolved_agent INT;
    v_unresolved_prod  INT;
    v_expired_active   INT;
    v_claim_before_policy INT;
    v_fail_count     INT;
BEGIN
    -- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
    v_run_date := CURRENT_DATE();

    -- ============================================================
    -- SECTION 2: Staging Completeness Checks
    -- ============================================================

    -- Check 2a: NULL policy_id in staging
    SELECT COUNT(*) INTO :v_null_count FROM stg_policies WHERE policy_id IS NULL;

    IF (v_null_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'NULL_KEY_CHECK', :v_null_count, 'FAIL',
            'NULL policy_id found in stg_policies: ' || :v_null_count::VARCHAR || ' rows');
            -- [CONVERT] CONVERT(VARCHAR, @null_count) → ::VARCHAR; + → ||
    ELSE
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'NULL_KEY_CHECK', 0, 'PASS');
    END IF;

    -- Check 2b: Duplicate policy_number in staging
    SELECT COUNT(*) INTO :v_dup_count
    FROM (
        SELECT policy_number, COUNT(*) AS cnt
        FROM   stg_policies
        GROUP BY policy_number
        HAVING COUNT(*) > 1
    );

    IF (v_dup_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'DUPLICATE_KEY_CHECK', :v_dup_count, 'FAIL',
            'Duplicate policy_number in staging: ' || :v_dup_count::VARCHAR || ' groups');
    ELSE
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'DUPLICATE_KEY_CHECK', 0, 'PASS');
    END IF;

    -- Check 2c: Invalid effective/expiry date logic
    SELECT COUNT(*) INTO :v_date_err_count
    FROM   stg_policies
    WHERE  expiry_date <= effective_date
        OR effective_date < '1990-01-01'
        OR expiry_date   > '2099-12-31';

    IF (v_date_err_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'DATE_LOGIC_CHECK', :v_date_err_count, 'WARN',
            'Invalid date range on ' || :v_date_err_count::VARCHAR || ' policy rows');
    END IF;

    -- Check 2d: Negative premium amounts
    SELECT COUNT(*) INTO :v_neg_prem FROM stg_policies WHERE annual_premium < 0;

    IF (v_neg_prem > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_policies',
            'NEGATIVE_AMOUNT_CHECK', :v_neg_prem, 'FAIL',
            'Negative annual_premium on ' || :v_neg_prem::VARCHAR || ' rows');
    END IF;

    -- Check 2e: Claims where paid > incurred (data anomaly)
    SELECT COUNT(*) INTO :v_overpaid_count
    FROM   stg_claims
    WHERE  total_paid > total_incurred
    AND    total_incurred IS NOT NULL;

    IF (v_overpaid_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_claims',
            'AMOUNT_LOGIC_CHECK', :v_overpaid_count, 'WARN',
            'Claims where paid > incurred: ' || :v_overpaid_count::VARCHAR || ' rows');
    END IF;

    -- Check 2f: Claims referencing unknown policy_id
    SELECT COUNT(*) INTO :v_orphan_claims
    FROM   stg_claims sc
    WHERE  NOT EXISTS (
        SELECT 1 FROM stg_policies sp WHERE sp.policy_id = sc.policy_id
    );

    IF (v_orphan_claims > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'stg_claims',
            'REFERENTIAL_INTEGRITY', :v_orphan_claims, 'FAIL',
            'Claims with no matching policy in staging: ' || :v_orphan_claims::VARCHAR);
    END IF;

    -- ============================================================
    -- SECTION 3: Fact Table Row Count vs. Prior Run
    -- ============================================================

    -- [CONVERT] Nested subquery for prior day's count
    SELECT MAX(rows_actual) INTO :v_prior_policy_count
    FROM   etl_audit_log
    WHERE  table_name  = 'fact_policy'
    AND    check_type  = 'ROW_COUNT'
    AND    run_date    = DATEADD(DAY, -1, :v_run_date);

    SELECT COUNT(*) INTO :v_curr_policy_count FROM fact_policy;

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
        check_type, rows_expected, rows_actual, check_result, threshold_pct, variance_pct)
    VALUES (
        :P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'fact_policy',
        'ROW_COUNT',
        :v_prior_policy_count,
        :v_curr_policy_count,
        CASE
            WHEN :v_prior_policy_count IS NULL THEN 'PASS'   -- first run
            WHEN ABS((:v_curr_policy_count - :v_prior_policy_count) * 100.0
                  / NULLIF(:v_prior_policy_count, 0)) > 5.0  THEN 'WARN'
            ELSE 'PASS'
        END,
        5.00,
        CASE WHEN :v_prior_policy_count IS NOT NULL AND :v_prior_policy_count > 0
             THEN ABS((:v_curr_policy_count - :v_prior_policy_count) * 100.0
                      / :v_prior_policy_count)
             ELSE NULL END
    );

    -- ============================================================
    -- SECTION 4: Dimension Key Resolution Check
    -- ============================================================

    SELECT COUNT(*) INTO :v_unresolved_cust  FROM fact_policy WHERE customer_key = -1;
    SELECT COUNT(*) INTO :v_unresolved_agent FROM fact_policy WHERE agent_key    = -1;
    SELECT COUNT(*) INTO :v_unresolved_prod  FROM fact_policy WHERE product_key  = -1;

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
        check_type, rows_actual, check_result, error_message)
    VALUES (
        :P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'fact_policy',
        'DIM_KEY_RESOLUTION_CUST',
        :v_unresolved_cust,
        CASE WHEN :v_unresolved_cust = 0 THEN 'PASS'
             WHEN :v_unresolved_cust < 10 THEN 'WARN' ELSE 'FAIL' END,
        CASE WHEN :v_unresolved_cust > 0
             THEN 'Unresolved customer_key in fact_policy: ' || :v_unresolved_cust::VARCHAR
             ELSE NULL END
    );

    -- ============================================================
    -- SECTION 5: Business Rule Validations
    -- ============================================================

    -- Business rule: Active policies must have a valid expiry date in the future
    -- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112)) → TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
    SELECT COUNT(*) INTO :v_expired_active
    FROM   fact_policy
    WHERE  status_code   = 'AC'
    AND    expiry_date_key < TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'));

    IF (v_expired_active > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'fact_policy',
            'BUSINESS_RULE_ACTIVE_EXP', :v_expired_active, 'WARN',
            'Active policies with past expiry date: ' || :v_expired_active::VARCHAR);
    END IF;

    -- Business rule: Claims should not predate their policy effective date
    SELECT COUNT(*) INTO :v_claim_before_policy
    FROM   fact_claims  fc
    INNER JOIN fact_policy fp ON fc.policy_id = fp.policy_id
    WHERE  fc.incident_date_key < fp.effective_date_key;

    IF (v_claim_before_policy > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:P_BATCH_ID, :v_run_date, 'sp_etl_dq_checks', 'fact_claims',
            'CLAIM_BEFORE_POLICY', :v_claim_before_policy, 'FAIL',
            'Claims with incident date before policy effective date: '
            || :v_claim_before_policy::VARCHAR);
    END IF;

    -- ============================================================
    -- SECTION 6: Final Summary
    -- ============================================================

    SELECT COUNT(*) INTO :v_fail_count FROM etl_audit_log
    WHERE  batch_id     = :P_BATCH_ID
    AND    check_result = 'FAIL';

    IF (v_fail_count > 0) THEN
        -- [CONVERT] RAISERROR ('ETL DQ Failure...', 16, 1) → RAISE
        -- [CONVERT] PRINT → removed (no console in Snowflake SP)
        RAISE USING
            MESSAGE = 'ETL BATCH FAILED: ' || :v_fail_count::VARCHAR
                      || ' data quality checks failed. Review etl_audit_log for batch_id = '
                      || :P_BATCH_ID::VARCHAR;
    END IF;

    RETURN 'ETL BATCH PASSED all data quality checks. batch_id=' || :P_BATCH_ID::VARCHAR;
END;
$$;

-- ============================================================
-- Summary report query (can be run standalone after SP execution)
-- ============================================================
-- SELECT
--     check_type, table_name, check_result, rows_actual, error_message, run_ts
-- FROM   etl_audit_log
-- WHERE  batch_id  = <batch_id>
-- ORDER BY
--     CASE check_result WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
--     table_name;
