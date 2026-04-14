-- =============================================================================
-- ETL SCRIPT 04 (ENHANCED): Data Quality Checks & Audit
-- Description : Comprehensive DQ validation using CHARINDEX, PATINDEX, STR(),
--               STUFF(), CURSOR-based row-level checks, dynamic SQL,
--               and COMPUTE BY audit summaries.
-- Complexity  : COMPLEX — Dynamic SQL, CURSOR, COMPUTE BY, string functions
-- =============================================================================

-- =============================================================================
-- SECTION 1: Audit Log Table (if not exists)
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'etl_audit_log' AND type = 'U')
BEGIN
    CREATE TABLE etl_audit_log (
        audit_id        INT             IDENTITY NOT NULL,
        script_name     VARCHAR(100)    NOT NULL,
        object_name     VARCHAR(100)    NOT NULL,
        check_type      VARCHAR(50)     NOT NULL,
        status          VARCHAR(20)     NOT NULL,  -- PASS, FAIL, WARNING
        row_count       INT             NULL,
        error_message   VARCHAR(2000)   NULL,
        run_ts          DATETIME        NOT NULL DEFAULT GETDATE(),
        run_by          VARCHAR(50)     NOT NULL DEFAULT USER_NAME(),
        PRIMARY KEY (audit_id)
    ) IN DBASPACE
END
GO

-- =============================================================================
-- SECTION 2: Standard DQ Checks (NULL, Duplicate, Referential Integrity)
-- =============================================================================

DECLARE @check_name     VARCHAR(100)
DECLARE @fail_count     INT
DECLARE @sql_stmt       VARCHAR(2000)
DECLARE @check_status   VARCHAR(20)

-- NULL check: critical columns in stg_policies
SELECT @check_name = 'STG_POLICIES_NULL_CRITICAL'

SELECT @fail_count = COUNT(*)
FROM stg_policies
WHERE policy_id IS NULL
   OR policy_number IS NULL
   OR customer_id IS NULL
   OR agent_id IS NULL
   OR effective_date IS NULL
   OR expiry_date IS NULL
   OR status_code IS NULL

SELECT @check_status = CASE WHEN @fail_count = 0 THEN 'PASS' ELSE 'FAIL' END

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_policies', @check_name, @check_status, @fail_count,
        CASE WHEN @fail_count > 0
             THEN STR(@fail_count, 6) + ' rows have NULL in critical columns'
             ELSE NULL END)

IF @fail_count > 0
    RAISERROR ('DQ FAIL: %s — %d rows with NULL critical columns', 10, 1, @check_name, @fail_count)
GO

-- Duplicate check: no duplicate policy_id in staging
DECLARE @dup_count INT

SELECT @dup_count = COUNT(*)
FROM (
    SELECT policy_id, COUNT(*) AS cnt
    FROM stg_policies
    GROUP BY policy_id
    HAVING COUNT(*) > 1
) dups

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_policies', 'DUPLICATE_POLICY_ID',
        CASE WHEN @dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @dup_count,
        CASE WHEN @dup_count > 0
             THEN CONVERT(VARCHAR, @dup_count) + ' duplicate policy_id values in staging'
             ELSE NULL END)
GO

-- Referential integrity: all stg_policies.customer_id must exist in stg_customers
DECLARE @ri_count INT

SELECT @ri_count = COUNT(*)
FROM stg_policies p
WHERE NOT EXISTS (
    SELECT 1 FROM stg_customers c WHERE c.customer_id = p.customer_id
)

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_policies', 'RI_CUSTOMER_EXISTS',
        CASE WHEN @ri_count = 0 THEN 'PASS' ELSE 'WARNING' END,
        @ri_count,
        CASE WHEN @ri_count > 0
             THEN CONVERT(VARCHAR, @ri_count) + ' policies reference non-existent customers'
             ELSE NULL END)
GO

-- Date logic check: expiry must be after effective
DECLARE @date_err INT

SELECT @date_err = COUNT(*)
FROM stg_policies
WHERE expiry_date <= effective_date

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_policies', 'DATE_RANGE_VALID',
        CASE WHEN @date_err = 0 THEN 'PASS' ELSE 'FAIL' END,
        @date_err,
        CASE WHEN @date_err > 0
             THEN CONVERT(VARCHAR, @date_err) + ' policies have expiry_date <= effective_date'
             ELSE NULL END)
GO

-- =============================================================================
-- SECTION 3: String Pattern Validation using PATINDEX and CHARINDEX
-- PATINDEX: Sybase pattern matching — migration: REGEXP_LIKE in Snowflake
-- CHARINDEX: find position — migration: POSITION() or CHARINDEX() (Snowflake supports)
-- =============================================================================

-- Check: zip_code must match 5-digit or ZIP+4 format
DECLARE @bad_zip INT

SELECT @bad_zip = COUNT(*)
FROM stg_customers
WHERE zip_code IS NOT NULL
  AND PATINDEX('[0-9][0-9][0-9][0-9][0-9]', zip_code) = 0        -- Not 5 digits
  AND PATINDEX('[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]', zip_code) = 0  -- Not ZIP+4

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_customers', 'ZIP_FORMAT_CHECK',
        CASE WHEN @bad_zip = 0 THEN 'PASS' ELSE 'WARNING' END,
        @bad_zip,
        CASE WHEN @bad_zip > 0
             THEN CONVERT(VARCHAR, @bad_zip) + ' customers have malformed zip_code'
             ELSE NULL END)
GO

-- Check: email must contain @ and a dot after @
DECLARE @bad_email INT

SELECT @bad_email = COUNT(*)
FROM stg_customers
WHERE email IS NOT NULL
  AND (
      CHARINDEX('@', email) = 0                          -- No @ sign
      OR CHARINDEX('@', email) = LEN(email)             -- @ is last char
      OR CHARINDEX('.', email, CHARINDEX('@', email)) = 0  -- No dot after @
  )
GO

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_customers', 'EMAIL_FORMAT_CHECK',
        CASE WHEN @bad_email = 0 THEN 'PASS' ELSE 'WARNING' END,
        @bad_email,
        CASE WHEN @bad_email > 0
             THEN CONVERT(VARCHAR, @bad_email) + ' customers have malformed email addresses'
             ELSE NULL END)
GO

-- Check: phone_primary should match NNN-NNN-NNNN or NNNNNNNNNN (10 digits)
DECLARE @bad_phone INT

SELECT @bad_phone = COUNT(*)
FROM stg_customers
WHERE phone_primary IS NOT NULL
  AND LEN(REPLACE(REPLACE(REPLACE(phone_primary, '-', ''), ' ', ''), '.', '')) <> 10
  AND PATINDEX('[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]',
               REPLACE(REPLACE(REPLACE(phone_primary, '-', ''), ' ', ''), '.', '')) = 0
GO

-- Check: bank routing number must be exactly 9 digits
DECLARE @bad_routing INT

SELECT @bad_routing = COUNT(*)
FROM stg_payments
WHERE bank_routing IS NOT NULL
  AND (
      LEN(bank_routing) <> 9
      OR PATINDEX('[^0-9]', bank_routing) > 0   -- Contains non-digit characters
  )

INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
VALUES ('etl_04_dq', 'stg_payments', 'BANK_ROUTING_FORMAT',
        CASE WHEN @bad_routing = 0 THEN 'PASS' ELSE 'WARNING' END,
        @bad_routing,
        CASE WHEN @bad_routing > 0
             THEN CONVERT(VARCHAR, @bad_routing) + ' payments have invalid bank routing numbers'
             ELSE NULL END)
GO

-- =============================================================================
-- SECTION 4: STUFF() for string building in DQ messages
-- STUFF(str, start, length, replacement) — Snowflake: use INSERT(str,start,len,rep)
-- =============================================================================

-- Build a comma-separated list of failed check names using STUFF
-- Pattern: STUFF removes leading comma from concatenated string
DECLARE @failed_checks  VARCHAR(2000)
DECLARE @fail_summary   VARCHAR(4000)

SELECT @failed_checks = ''

SELECT @failed_checks = @failed_checks + ', ' + check_type
FROM etl_audit_log
WHERE run_ts >= DATEADD(minute, -10, GETDATE())
  AND status = 'FAIL'
ORDER BY audit_id

-- STUFF removes the leading ", " (2 chars from position 1)
IF LEN(@failed_checks) > 0
    SELECT @failed_checks = STUFF(@failed_checks, 1, 2, '')

-- Build formatted summary report using STR() and STUFF()
-- STR(number, length, decimal) — Snowflake: TO_CHAR(number, format)
SELECT @fail_summary =
    'DQ Run Summary: '
    + STR((SELECT COUNT(*) FROM etl_audit_log
           WHERE run_ts >= DATEADD(minute,-10,GETDATE()) AND status = 'PASS'), 4)
    + ' passed, '
    + STR((SELECT COUNT(*) FROM etl_audit_log
           WHERE run_ts >= DATEADD(minute,-10,GETDATE()) AND status = 'WARNING'), 4)
    + ' warnings, '
    + STR((SELECT COUNT(*) FROM etl_audit_log
           WHERE run_ts >= DATEADD(minute,-10,GETDATE()) AND status = 'FAIL'), 4)
    + ' failures.'

PRINT @fail_summary

IF LEN(ISNULL(@failed_checks,'')) > 0
BEGIN
    PRINT 'Failed checks: ' + @failed_checks
    RAISERROR ('DQ FAILURES DETECTED: %s', 16, 1, @failed_checks)
END
GO

-- =============================================================================
-- SECTION 5: CURSOR-based Row-Level DQ for Claims
-- Checks each claim for business rule violations that require row context
-- CURSOR is Sybase-specific — migration: set-based CASE/WHERE in Snowflake
-- =============================================================================

DECLARE @c_claim_id         BIGINT
DECLARE @c_incident_date    DATE
DECLARE @c_reported_date    DATE
DECLARE @c_closed_date      DATE
DECLARE @c_total_incurred   DECIMAL(18,2)
DECLARE @c_total_paid       DECIMAL(18,2)
DECLARE @c_total_reserved   DECIMAL(18,2)
DECLARE @c_status_code      VARCHAR(10)
DECLARE @c_litigation_flag  CHAR(1)
DECLARE @c_error_parts      VARCHAR(500)
DECLARE @c_error_msg        VARCHAR(2000)
DECLARE @c_fail_total       INT
DECLARE @c_warn_total       INT

SELECT @c_fail_total = 0
SELECT @c_warn_total = 0

DECLARE claims_dq_cursor CURSOR FOR
    SELECT
        claim_id, incident_date, reported_date, closed_date,
        total_incurred, total_paid, total_reserved,
        status_code, litigation_flag
    FROM stg_claims
    ORDER BY claim_id
FOR READ ONLY

OPEN claims_dq_cursor

FETCH NEXT FROM claims_dq_cursor
    INTO @c_claim_id, @c_incident_date, @c_reported_date, @c_closed_date,
         @c_total_incurred, @c_total_paid, @c_total_reserved,
         @c_status_code, @c_litigation_flag

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @c_error_parts = ''

    -- Rule 1: reported_date must be >= incident_date
    IF @c_reported_date < @c_incident_date
        SELECT @c_error_parts = @c_error_parts + '|R01:reported<incident'

    -- Rule 2: closed_date must be >= reported_date (if present)
    IF @c_closed_date IS NOT NULL AND @c_closed_date < @c_reported_date
        SELECT @c_error_parts = @c_error_parts + '|R02:closed<reported'

    -- Rule 3: total_paid cannot exceed total_incurred (fraud flag)
    IF ISNULL(@c_total_paid, 0) > ISNULL(@c_total_incurred, 0) * 1.05  -- 5% tolerance
        SELECT @c_error_parts = @c_error_parts + '|R03:paid>incurred'

    -- Rule 4: closed claims must have a closed_date
    IF @c_status_code = 'CL' AND @c_closed_date IS NULL
        SELECT @c_error_parts = @c_error_parts + '|R04:closed_no_date'

    -- Rule 5: litigated claims should have reserve > 0
    IF @c_litigation_flag = 'Y' AND ISNULL(@c_total_reserved, 0) = 0
        SELECT @c_error_parts = @c_error_parts + '|R05:litigation_no_reserve'

    -- Rule 6: report lag > 365 days is suspicious
    IF DATEDIFF(day, @c_incident_date, @c_reported_date) > 365
        SELECT @c_error_parts = @c_error_parts + '|R06:lag>365days'

    -- Log violations
    IF LEN(@c_error_parts) > 0
    BEGIN
        -- Remove leading pipe using STUFF
        SELECT @c_error_msg = 'Claim ' + CONVERT(VARCHAR, @c_claim_id)
            + ': ' + STUFF(@c_error_parts, 1, 1, '')

        INSERT INTO etl_audit_log (
            script_name, object_name, check_type, status, row_count, error_message
        )
        VALUES (
            'etl_04_dq', 'stg_claims', 'CLAIM_BUSINESS_RULES',
            CASE WHEN @c_error_parts LIKE '%R03%' OR @c_error_parts LIKE '%R04%'
                 THEN 'FAIL' ELSE 'WARNING' END,
            1,
            @c_error_msg
        )

        IF @c_error_parts LIKE '%R03%' OR @c_error_parts LIKE '%R04%'
            SELECT @c_fail_total = @c_fail_total + 1
        ELSE
            SELECT @c_warn_total = @c_warn_total + 1
    END

    FETCH NEXT FROM claims_dq_cursor
        INTO @c_claim_id, @c_incident_date, @c_reported_date, @c_closed_date,
             @c_total_incurred, @c_total_paid, @c_total_reserved,
             @c_status_code, @c_litigation_flag
END

CLOSE     claims_dq_cursor
DEALLOCATE claims_dq_cursor

PRINT 'Claims DQ cursor complete. Failures: ' + CONVERT(VARCHAR, @c_fail_total)
      + ', Warnings: ' + CONVERT(VARCHAR, @c_warn_total)

IF @c_fail_total > 0
    RAISERROR ('DQ FAIL: %d claims failed business rule validation.', 16, 1, @c_fail_total)
GO

-- =============================================================================
-- SECTION 6: Dynamic SQL for Parameterized Row Count Variance Check
-- EXEC(@sql) — dynamic SQL — no direct equivalent in Snowflake
-- Migration: use Snowflake Scripting EXECUTE IMMEDIATE
-- =============================================================================

DECLARE @table_name     VARCHAR(100)
DECLARE @threshold_pct  DECIMAL(5,2)
DECLARE @curr_count     INT
DECLARE @prev_count     INT
DECLARE @variance_pct   DECIMAL(10,4)
DECLARE @dyn_sql        VARCHAR(2000)
DECLARE @dyn_result     TABLE (cnt INT)

-- Check variance for each staging table dynamically
DECLARE table_cursor CURSOR FOR
    SELECT 'stg_policies',  10.0   UNION ALL
    SELECT 'stg_customers', 15.0   UNION ALL
    SELECT 'stg_claims',    25.0   UNION ALL
    SELECT 'stg_payments',  30.0
FOR READ ONLY

OPEN table_cursor
FETCH NEXT FROM table_cursor INTO @table_name, @threshold_pct

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Dynamic SQL: build count query for current table
    -- EXEC(@dyn_sql) — Sybase dynamic execution
    SELECT @dyn_sql = 'SELECT COUNT(*) FROM ' + @table_name
    EXEC(@dyn_sql)   -- Result goes to output; in production, use temp table

    -- Simulate: get current count directly (temp table pattern)
    SELECT @dyn_sql = 'SELECT @out = COUNT(*) FROM ' + @table_name
    -- In Sybase IQ, variables cannot be set inside EXEC directly
    -- Pattern: use temp table as intermediary
    IF OBJECT_ID('tempdb..#row_count_tmp') IS NOT NULL
        DROP TABLE #row_count_tmp

    SELECT @dyn_sql =
        'SELECT COUNT(*) AS cnt INTO #row_count_tmp FROM ' + @table_name
    EXEC(@dyn_sql)

    SELECT @curr_count = cnt FROM #row_count_tmp
    DROP TABLE #row_count_tmp

    -- Get previous run count from audit log
    SELECT @prev_count = row_count
    FROM etl_audit_log
    WHERE object_name  = @table_name
      AND check_type   = 'ROW_COUNT_VARIANCE'
      AND status       = 'PASS'
      AND audit_id = (
          SELECT MAX(audit_id) FROM etl_audit_log
          WHERE object_name = @table_name
            AND check_type  = 'ROW_COUNT_VARIANCE'
      )

    -- Calculate variance
    IF ISNULL(@prev_count, 0) > 0
    BEGIN
        SELECT @variance_pct = ABS(CAST(@curr_count - @prev_count AS DECIMAL(18,4))
                                / @prev_count * 100.0)

        INSERT INTO etl_audit_log (
            script_name, object_name, check_type, status, row_count, error_message
        )
        VALUES (
            'etl_04_dq', @table_name, 'ROW_COUNT_VARIANCE',
            CASE WHEN @variance_pct > @threshold_pct THEN 'WARNING' ELSE 'PASS' END,
            @curr_count,
            'Variance: ' + CONVERT(VARCHAR, ROUND(@variance_pct, 2))
            + '% (threshold: ' + CONVERT(VARCHAR, @threshold_pct) + '%)'
            + ' Prev: ' + CONVERT(VARCHAR, @prev_count)
            + ' Curr: ' + CONVERT(VARCHAR, @curr_count)
        )
    END
    ELSE
    BEGIN
        -- First run — just log count, no variance check possible
        INSERT INTO etl_audit_log (
            script_name, object_name, check_type, status, row_count, error_message
        )
        VALUES ('etl_04_dq', @table_name, 'ROW_COUNT_VARIANCE', 'PASS',
                @curr_count, 'First run — baseline established')
    END

    FETCH NEXT FROM table_cursor INTO @table_name, @threshold_pct
END

CLOSE     table_cursor
DEALLOCATE table_cursor
GO

-- =============================================================================
-- SECTION 7: COMPUTE BY — Audit Summary by Script and Status
-- COMPUTE BY generates subtotals — migration: GROUP BY ROLLUP or window function
-- =============================================================================

-- Full audit run summary with subtotals per check_type
SELECT
    check_type,
    status,
    COUNT(*)    AS check_count,
    SUM(ISNULL(row_count, 0)) AS total_rows_flagged
FROM etl_audit_log
WHERE run_ts >= DATEADD(hour, -1, GETDATE())
GROUP BY check_type, status
ORDER BY check_type, status
COMPUTE
    SUM(COUNT(*)),
    SUM(SUM(ISNULL(row_count, 0)))
BY check_type
GO

-- Overall totals
SELECT
    status,
    COUNT(*)    AS check_count,
    SUM(ISNULL(row_count, 0)) AS total_flagged_rows
FROM etl_audit_log
WHERE run_ts >= DATEADD(hour, -1, GETDATE())
GROUP BY status
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END
COMPUTE
    SUM(COUNT(*)),
    SUM(SUM(ISNULL(row_count, 0)))
GO

PRINT 'Script 04 complete: DQ checks and audit log updated'
GO
