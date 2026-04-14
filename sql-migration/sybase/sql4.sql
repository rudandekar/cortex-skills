-- ============================================================================= 

-- ETL SCRIPT 04: Data Quality Checks & ETL Audit Logging 

-- Description : Validates data completeness, referential integrity, and 

--               business rules across staging and fact/dim tables. 

--               Logs run results to etl_audit_log for monitoring. 

-- Target DB   : DataWarehouse (Sybase IQ) 

-- Author      : DW Team 

-- Created     : 2010-01-12 

-- Modified    : 2014-08-30 

-- ============================================================================= 

  

-- ============================================================ 

-- SECTION 1: ETL Audit Log Table (create if not exists) 

-- ============================================================ 

  

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'etl_audit_log' AND type = 'U') 

BEGIN 

    CREATE TABLE etl_audit_log ( 

        audit_id            INT IDENTITY(1,1)   NOT NULL PRIMARY KEY, 

        batch_id            INT                 NOT NULL, 

        run_date            DATE                NOT NULL, 

        run_ts              DATETIME            NOT NULL DEFAULT GETDATE(), 

        script_name         VARCHAR(100)        NOT NULL, 

        table_name          VARCHAR(100)        NOT NULL, 

        check_type          VARCHAR(50)         NOT NULL, 

        rows_expected       INT                 NULL, 

        rows_actual         INT                 NULL, 

        check_result        VARCHAR(10)         NOT NULL,  -- PASS / FAIL / WARN 

        threshold_pct       DECIMAL(6,2)        NULL, 

        variance_pct        DECIMAL(8,4)        NULL, 

        error_message       VARCHAR(500)        NULL, 

        created_ts          DATETIME            NOT NULL DEFAULT GETDATE() 

    ) 

END 

GO 

  

-- Working variables 

DECLARE @batch_id       INT 

DECLARE @run_date       DATE 

DECLARE @stg_count      INT 

DECLARE @fact_count     INT 

DECLARE @dim_count      INT 

DECLARE @null_count     INT 

DECLARE @dup_count      INT 

DECLARE @variance_pct   DECIMAL(8,4) 

DECLARE @threshold      DECIMAL(6,2) 

  

SELECT @batch_id   = 1001 

SELECT @run_date   = CAST(GETDATE() AS DATE) 

SELECT @threshold  = 5.00   -- 5% variance threshold for row counts 

  

-- ============================================================ 

-- SECTION 2: Staging Completeness Checks 

-- ============================================================ 

  

-- Check 2a: NULL policy_id in staging 

SELECT @null_count = COUNT(*) FROM stg_policies WHERE policy_id IS NULL 

IF @null_count > 0 

BEGIN 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (@batch_id, @run_date, 'etl_04_dq_checks.sql', 'stg_policies', 

        'NULL_KEY_CHECK', @null_count, 'FAIL', 

        'NULL policy_id found in stg_policies: ' + CONVERT(VARCHAR, @null_count) + ' rows') 

    PRINT 'FAIL: NULL policy_id in stg_policies = ' + CONVERT(VARCHAR, @null_count) 

END 

ELSE 

BEGIN 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result) 

    VALUES (@batch_id, @run_date, 'etl_04_dq_checks.sql', 'stg_policies', 

        'NULL_KEY_CHECK', 0, 'PASS') 

END 

GO 

  

-- Check 2b: Duplicate policy_number in staging 

DECLARE @dup_count INT 

SELECT @dup_count = COUNT(*) 

FROM ( 

    SELECT policy_number, COUNT(*) AS cnt 

    FROM   stg_policies 

    GROUP BY policy_number 

    HAVING COUNT(*) > 1 

) dups 

  

IF @dup_count > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_policies', 

        'DUPLICATE_KEY_CHECK', @dup_count, 'FAIL', 

        'Duplicate policy_number in staging: ' + CONVERT(VARCHAR, @dup_count) + ' groups') 

ELSE 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_policies', 

        'DUPLICATE_KEY_CHECK', 0, 'PASS') 

GO 

  

-- Check 2c: Invalid effective/expiry date logic 

DECLARE @date_err_count INT 

SELECT @date_err_count = COUNT(*) 

FROM   stg_policies 

WHERE  expiry_date <= effective_date 

    OR effective_date < '1990-01-01' 

    OR expiry_date   > '2099-12-31' 

  

IF @date_err_count > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_policies', 

        'DATE_LOGIC_CHECK', @date_err_count, 'WARN', 

        'Invalid date range on ' + CONVERT(VARCHAR, @date_err_count) + ' policy rows') 

GO 

  

-- Check 2d: Negative premium amounts 

DECLARE @neg_prem INT 

SELECT @neg_prem = COUNT(*) FROM stg_policies WHERE annual_premium < 0 

  

IF @neg_prem > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_policies', 

        'NEGATIVE_AMOUNT_CHECK', @neg_prem, 'FAIL', 

        'Negative annual_premium on ' + CONVERT(VARCHAR, @neg_prem) + ' rows') 

GO 

  

-- Check 2e: Claims where paid > incurred (data anomaly) 

DECLARE @overpaid_count INT 

SELECT @overpaid_count = COUNT(*) 

FROM   stg_claims 

WHERE  total_paid > total_incurred 

AND    total_incurred IS NOT NULL 

  

IF @overpaid_count > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_claims', 

        'AMOUNT_LOGIC_CHECK', @overpaid_count, 'WARN', 

        'Claims where paid > incurred: ' + CONVERT(VARCHAR, @overpaid_count) + ' rows') 

GO 

  

-- Check 2f: Claims referencing unknown policy_id 

DECLARE @orphan_claims INT 

SELECT @orphan_claims = COUNT(*) 

FROM   stg_claims sc 

WHERE  NOT EXISTS ( 

    SELECT 1 FROM stg_policies sp WHERE sp.policy_id = sc.policy_id 

) 

  

IF @orphan_claims > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'stg_claims', 

        'REFERENTIAL_INTEGRITY', @orphan_claims, 'FAIL', 

        'Claims with no matching policy in staging: ' + CONVERT(VARCHAR, @orphan_claims)) 

GO 

  

-- ============================================================ 

-- SECTION 3: Fact Table Row Count vs. Prior Run 

-- ============================================================ 

  

-- Compare current run to previous day's count (5% threshold) 

DECLARE @prior_policy_count INT 

DECLARE @curr_policy_count  INT 

DECLARE @pct_change         DECIMAL(8,4) 

  

SELECT @prior_policy_count = rows_actual 

FROM   etl_audit_log 

WHERE  table_name  = 'fact_policy' 

AND    check_type  = 'ROW_COUNT' 

AND    run_date    = DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) 

AND    audit_id    = (SELECT MAX(audit_id) FROM etl_audit_log 

                      WHERE table_name = 'fact_policy' 

                      AND   check_type = 'ROW_COUNT' 

                      AND   run_date   = DATEADD(DAY, -1, CAST(GETDATE() AS DATE))) 

  

SELECT @curr_policy_count = COUNT(*) FROM fact_policy 

  

INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

    check_type, rows_expected, rows_actual, check_result, threshold_pct, variance_pct) 

VALUES ( 

    1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'fact_policy', 

    'ROW_COUNT', 

    @prior_policy_count, 

    @curr_policy_count, 

    CASE 

        WHEN @prior_policy_count IS NULL THEN 'PASS'   -- first run 

        WHEN ABS((@curr_policy_count - @prior_policy_count) * 100.0 

              / NULLIF(@prior_policy_count, 0)) > 5.0  THEN 'WARN' 

        ELSE 'PASS' 

    END, 

    5.00, 

    CASE WHEN @prior_policy_count IS NOT NULL AND @prior_policy_count > 0 

         THEN ABS((@curr_policy_count - @prior_policy_count) * 100.0 

                  / @prior_policy_count) 

         ELSE NULL END 

) 

GO 

  

-- ============================================================ 

-- SECTION 4: Dimension Key Resolution Check 

-- ============================================================ 

  

-- Unresolved dimension keys in fact_policy 

DECLARE @unresolved_cust INT 

DECLARE @unresolved_agent INT 

DECLARE @unresolved_prod  INT 

  

SELECT @unresolved_cust  = COUNT(*) FROM fact_policy WHERE customer_key = -1 

SELECT @unresolved_agent = COUNT(*) FROM fact_policy WHERE agent_key    = -1 

SELECT @unresolved_prod  = COUNT(*) FROM fact_policy WHERE product_key  = -1 

  

-- Customer key resolution rate 

INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

    check_type, rows_actual, check_result, error_message) 

VALUES ( 

    1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'fact_policy', 

    'DIM_KEY_RESOLUTION_CUST', 

    @unresolved_cust, 

    CASE WHEN @unresolved_cust = 0 THEN 'PASS' 

         WHEN @unresolved_cust < 10 THEN 'WARN' ELSE 'FAIL' END, 

    CASE WHEN @unresolved_cust > 0 

         THEN 'Unresolved customer_key in fact_policy: ' + CONVERT(VARCHAR, @unresolved_cust) 

         ELSE NULL END 

) 

GO 

  

-- ============================================================ 

-- SECTION 5: Business Rule Validations 

-- ============================================================ 

  

-- Business rule: Active policies must have a valid expiry date in the future 

DECLARE @expired_active INT 

SELECT @expired_active = COUNT(*) 

FROM   fact_policy 

WHERE  status_code   = 'AC' 

AND    expiry_date_key < CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112)) 

  

IF @expired_active > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'fact_policy', 

        'BUSINESS_RULE_ACTIVE_EXP', @expired_active, 'WARN', 

        'Active policies with past expiry date: ' + CONVERT(VARCHAR, @expired_active)) 

GO 

  

-- Business rule: Claims should not predate their policy effective date 

DECLARE @claim_before_policy INT 

SELECT @claim_before_policy = COUNT(*) 

FROM   fact_claims  fc 

INNER JOIN fact_policy fp ON fc.policy_id = fp.policy_id 

WHERE  fc.incident_date_key < fp.effective_date_key 

  

IF @claim_before_policy > 0 

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name, 

        check_type, rows_actual, check_result, error_message) 

    VALUES (1001, CAST(GETDATE() AS DATE), 'etl_04_dq_checks.sql', 'fact_claims', 

        'CLAIM_BEFORE_POLICY', @claim_before_policy, 'FAIL', 

        'Claims with incident date before policy effective date: ' 

        + CONVERT(VARCHAR, @claim_before_policy)) 

GO 

  

-- ============================================================ 

-- SECTION 6: Final Summary Report 

-- ============================================================ 

  

SELECT 

    check_type, 

    table_name, 

    check_result, 

    rows_actual, 

    error_message, 

    run_ts 

FROM   etl_audit_log 

WHERE  batch_id  = 1001 

ORDER BY 

    CASE check_result WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END, 

    table_name 

GO 

  

-- Fail the batch if any FAIL results exist 

DECLARE @fail_count INT 

SELECT  @fail_count = COUNT(*) FROM etl_audit_log 

WHERE   batch_id     = 1001 

AND     check_result = 'FAIL' 

  

IF @fail_count > 0 

BEGIN 

    PRINT 'ETL BATCH FAILED: ' + CONVERT(VARCHAR, @fail_count) + ' data quality checks failed.' 

    PRINT 'Review etl_audit_log for batch_id = 1001' 

    -- In production: RAISERROR would trigger alerting 

    -- RAISERROR ('ETL DQ Failure - see audit log', 16, 1) 

END 

ELSE 

    PRINT 'ETL BATCH PASSED all data quality checks.' 

GO 

  
