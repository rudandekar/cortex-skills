-- =============================================================================
-- COMPLEX-TIER CONVERTED OBJECTS (12 objects)
-- Converted from: Sybase ASE / IQ → Snowflake
-- Source files  : sql4.sql, ETL_Script_04.sql, ETL_Script_05.sql,
--                 ETL_Script_06.sql, ETL_Script_07.sql
-- Converter     : Cortex Code (sql-migration skill)
-- Date          : 2026-04-02
-- =============================================================================
-- Type-map reference:
--   DATETIME       → TIMESTAMP_NTZ                       [CONVERT]
--   TINYINT        → SMALLINT                            [CONVERT]
--   MONEY          → NUMBER(19,4)                        [CONVERT]
--   DECIMAL(n,m)   → NUMBER(n,m)                         [CONVERT]
--   IDENTITY(1,1)  → AUTOINCREMENT                       [CONVERT]
--   GETDATE()      → CURRENT_TIMESTAMP()                 [CONVERT]
--   ISNULL(a,b)    → COALESCE(a,b)                       [CONVERT]
--   IN DBASPACE    → (removed)                           [REMOVE]
--   IQ UNIQUE(n)   → (removed)                           [REMOVE]
--   GO             → (removed, use ;)                    [REMOVE]
--   PRINT          → SYSTEM$LOG / removed                [REMOVE]
--   CONVERT(VARCHAR,x)  → CAST(x AS VARCHAR) / TO_CHAR   [CONVERT]
--   PATINDEX(pat,str)   → REGEXP_LIKE / REGEXP_INSTR     [CONVERT]
--   CHARINDEX(s,str)    → POSITION(s IN str)             [CONVERT]
--   LEN(str)            → LENGTH(str)                    [CONVERT]
--   STUFF(str,1,n,'')   → SUBSTR(str, n+1)              [CONVERT]
--   STR(num,len)        → LPAD(TO_CHAR(num), len)        [CONVERT]
--   USER_NAME()         → CURRENT_USER()                 [CONVERT]
--   @@ERROR             → EXCEPTION WHEN OTHER THEN       [CONVERT]
--   @@ROWCOUNT           → SQLROWCOUNT                    [CONVERT]
--   @@FETCH_STATUS       → set-based rewrite              [CONVERT]
--   RAISERROR            → RAISE / exception block        [CONVERT]
--   EXEC(@sql)           → EXECUTE IMMEDIATE :sql         [CONVERT]
--   EXEC proc @p=v       → CALL proc(v)                  [CONVERT]
--   COMPUTE BY           → GROUP BY ROLLUP()             [REMOVE]
--   BEGIN TRAN/COMMIT    → BEGIN TRANSACTION/COMMIT       [CONVERT]
--   GOTO label           → restructured as IF/ELSE        [CONVERT]
--   DATEDIFF(part,a,b)   → DATEDIFF('part',a,b)          [CONVERT]
--   DATEADD(part,n,d)    → DATEADD('part',n,d)            [CONVERT]
--   @var = @var + ', ' + col → LISTAGG(col, ', ')         [CONVERT]
--   CREATE RULE          → CHECK constraint               [CONVERT]
--   CREATE DEFAULT       → column DEFAULT                 [CONVERT]
--   sp_bindrule          → (removed / inlined)            [REMOVE]
--   sp_binddefault       → (removed / inlined)            [REMOVE]
--   SET TEMPORARY OPTION → (removed)                      [REMOVE]
--   OBJECT_ID('tempdb..#t') → CREATE OR REPLACE TEMP TABLE [CONVERT]
-- =============================================================================


-- #############################################################################
-- OBJECT 1 of 12: etl_audit_log + DQ checks (basic series, sql4.sql)
-- #############################################################################

-- =============================================================================
-- Source : sql4.sql lines 1-593
-- Object : etl_audit_log DDL + DQ check script (anonymous block)
-- Series : basic
-- Layer  : audit_dq
-- Score  : 7 (Complex)
-- Changes: IF NOT EXISTS ... CREATE TABLE → CREATE TABLE IF NOT EXISTS  [CONVERT]
--          IDENTITY(1,1) → AUTOINCREMENT                               [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                                    [CONVERT]
--          DECIMAL(n,m) → NUMBER(n,m)                                  [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                             [CONVERT]
--          DECLARE @var → LET var in DECLARE block                     [CONVERT]
--          SELECT @var = expr → var := (SELECT expr)                   [CONVERT]
--          CONVERT(VARCHAR, @val) → TO_CHAR(:val)                      [CONVERT]
--          PRINT → SYSTEM$LOG                                          [CONVERT]
--          CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112)) →
--              TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD'))     [CONVERT]
--          NULLIF(@prior, 0) → NULLIF(:prior, 0)                      [CONVERT]
--          Multiple GO-separated blocks → single Snowflake Scripting   [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- DDL: etl_audit_log (basic version)
-- [CONVERT] IF NOT EXISTS ... BEGIN CREATE TABLE ... END → CREATE TABLE IF NOT EXISTS
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [CONVERT] DECIMAL(6,2) → NUMBER(6,2); DECIMAL(8,4) → NUMBER(8,4)
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [REMOVE] GO
CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id            INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    batch_id            INT             NOT NULL,
    run_date            DATE            NOT NULL,
    run_ts              TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    script_name         VARCHAR(100)    NOT NULL,
    table_name          VARCHAR(100)    NOT NULL,
    check_type          VARCHAR(50)     NOT NULL,
    rows_expected       INT             NULL,
    rows_actual         INT             NULL,
    check_result        VARCHAR(10)     NOT NULL,       -- PASS / FAIL / WARN
    threshold_pct       NUMBER(6,2)     NULL,
    variance_pct        NUMBER(8,4)     NULL,
    error_message       VARCHAR(500)    NULL,
    created_ts          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- DQ checks as a single Snowflake Scripting anonymous block
-- [CONVERT] All DECLARE @var TYPE → LET var TYPE in DECLARE block
-- [CONVERT] SELECT @var = expr → var := (SELECT expr FROM ...)
-- [CONVERT] CONVERT(VARCHAR, @val) → TO_CHAR(:val)
-- [CONVERT] PRINT → SYSTEM$LOG('INFO', ...)
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112)) → TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD'))
-- [CONVERT] DATEADD(DAY, -1, ...) → DATEADD('DAY', -1, ...)
-- [CONVERT] NULLIF(@prior, 0) → NULLIF(:prior, 0)
-- [REMOVE] GO — batch separators removed; combined into single block
DECLARE
    batch_id            INT := 1001;
    run_date            DATE := CURRENT_DATE();
    threshold           NUMBER(6,2) := 5.00;
    stg_count           INT;
    fact_count          INT;
    dim_count           INT;
    null_count          INT;
    dup_count           INT;
    variance_pct        NUMBER(8,4);
    date_err_count      INT;
    neg_prem            INT;
    overpaid_count      INT;
    orphan_claims       INT;
    prior_policy_count  INT;
    curr_policy_count   INT;
    pct_change          NUMBER(8,4);
    unresolved_cust     INT;
    unresolved_agent    INT;
    unresolved_prod     INT;
    expired_active      INT;
    claim_before_policy INT;
    fail_count          INT;
BEGIN

    -- ============================================================
    -- SECTION 2: Staging Completeness Checks
    -- ============================================================

    -- Check 2a: NULL policy_id in staging
    SELECT COUNT(*) INTO :null_count FROM stg_policies WHERE policy_id IS NULL;

    IF (null_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'NULL_KEY_CHECK', :null_count, 'FAIL',
            'NULL policy_id found in stg_policies: ' || TO_CHAR(:null_count) || ' rows');
        SYSTEM$LOG('INFO', 'FAIL: NULL policy_id in stg_policies = ' || TO_CHAR(:null_count));
    ELSE
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'NULL_KEY_CHECK', 0, 'PASS');
    END IF;

    -- Check 2b: Duplicate policy_number in staging
    SELECT COUNT(*) INTO :dup_count
    FROM (
        SELECT policy_number, COUNT(*) AS cnt
        FROM   stg_policies
        GROUP BY policy_number
        HAVING COUNT(*) > 1
    ) dups;

    IF (dup_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'DUPLICATE_KEY_CHECK', :dup_count, 'FAIL',
            'Duplicate policy_number in staging: ' || TO_CHAR(:dup_count) || ' groups');
    ELSE
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'DUPLICATE_KEY_CHECK', 0, 'PASS');
    END IF;

    -- Check 2c: Invalid effective/expiry date logic
    SELECT COUNT(*) INTO :date_err_count
    FROM   stg_policies
    WHERE  expiry_date <= effective_date
        OR effective_date < '1990-01-01'
        OR expiry_date   > '2099-12-31';

    IF (date_err_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'DATE_LOGIC_CHECK', :date_err_count, 'WARN',
            'Invalid date range on ' || TO_CHAR(:date_err_count) || ' policy rows');
    END IF;

    -- Check 2d: Negative premium amounts
    SELECT COUNT(*) INTO :neg_prem FROM stg_policies WHERE annual_premium < 0;

    IF (neg_prem > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_policies',
            'NEGATIVE_AMOUNT_CHECK', :neg_prem, 'FAIL',
            'Negative annual_premium on ' || TO_CHAR(:neg_prem) || ' rows');
    END IF;

    -- Check 2e: Claims where paid > incurred (data anomaly)
    SELECT COUNT(*) INTO :overpaid_count
    FROM   stg_claims
    WHERE  total_paid > total_incurred
    AND    total_incurred IS NOT NULL;

    IF (overpaid_count > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_claims',
            'AMOUNT_LOGIC_CHECK', :overpaid_count, 'WARN',
            'Claims where paid > incurred: ' || TO_CHAR(:overpaid_count) || ' rows');
    END IF;

    -- Check 2f: Claims referencing unknown policy_id
    SELECT COUNT(*) INTO :orphan_claims
    FROM   stg_claims sc
    WHERE  NOT EXISTS (
        SELECT 1 FROM stg_policies sp WHERE sp.policy_id = sc.policy_id
    );

    IF (orphan_claims > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'stg_claims',
            'REFERENTIAL_INTEGRITY', :orphan_claims, 'FAIL',
            'Claims with no matching policy in staging: ' || TO_CHAR(:orphan_claims));
    END IF;

    -- ============================================================
    -- SECTION 3: Fact Table Row Count vs. Prior Run
    -- ============================================================

    -- [CONVERT] DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) → DATEADD('DAY', -1, CURRENT_DATE())
    SELECT rows_actual INTO :prior_policy_count
    FROM   etl_audit_log
    WHERE  table_name  = 'fact_policy'
    AND    check_type  = 'ROW_COUNT'
    AND    run_date    = DATEADD('DAY', -1, CURRENT_DATE())
    AND    audit_id    = (SELECT MAX(audit_id) FROM etl_audit_log
                          WHERE table_name = 'fact_policy'
                          AND   check_type = 'ROW_COUNT'
                          AND   run_date   = DATEADD('DAY', -1, CURRENT_DATE()));

    SELECT COUNT(*) INTO :curr_policy_count FROM fact_policy;

    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
        check_type, rows_expected, rows_actual, check_result, threshold_pct, variance_pct)
    VALUES (
        :batch_id, :run_date, 'etl_04_dq_checks.sql', 'fact_policy',
        'ROW_COUNT',
        :prior_policy_count,
        :curr_policy_count,
        CASE
            WHEN :prior_policy_count IS NULL THEN 'PASS'
            WHEN ABS((:curr_policy_count - :prior_policy_count) * 100.0
                  / NULLIF(:prior_policy_count, 0)) > 5.0  THEN 'WARN'
            ELSE 'PASS'
        END,
        5.00,
        CASE WHEN :prior_policy_count IS NOT NULL AND :prior_policy_count > 0
             THEN ABS((:curr_policy_count - :prior_policy_count) * 100.0
                      / :prior_policy_count)
             ELSE NULL END
    );

    -- ============================================================
    -- SECTION 4: Dimension Key Resolution Check
    -- ============================================================

    SELECT COUNT(*) INTO :unresolved_cust  FROM fact_policy WHERE customer_key = -1;
    SELECT COUNT(*) INTO :unresolved_agent FROM fact_policy WHERE agent_key    = -1;
    SELECT COUNT(*) INTO :unresolved_prod  FROM fact_policy WHERE product_key  = -1;

    -- Customer key resolution rate
    INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
        check_type, rows_actual, check_result, error_message)
    VALUES (
        :batch_id, :run_date, 'etl_04_dq_checks.sql', 'fact_policy',
        'DIM_KEY_RESOLUTION_CUST',
        :unresolved_cust,
        CASE WHEN :unresolved_cust = 0 THEN 'PASS'
             WHEN :unresolved_cust < 10 THEN 'WARN' ELSE 'FAIL' END,
        CASE WHEN :unresolved_cust > 0
             THEN 'Unresolved customer_key in fact_policy: ' || TO_CHAR(:unresolved_cust)
             ELSE NULL END
    );

    -- ============================================================
    -- SECTION 5: Business Rule Validations
    -- ============================================================

    -- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112)) → TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
    SELECT COUNT(*) INTO :expired_active
    FROM   fact_policy
    WHERE  status_code   = 'AC'
    AND    expiry_date_key < TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'));

    IF (expired_active > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'fact_policy',
            'BUSINESS_RULE_ACTIVE_EXP', :expired_active, 'WARN',
            'Active policies with past expiry date: ' || TO_CHAR(:expired_active));
    END IF;

    -- Business rule: Claims should not predate their policy effective date
    SELECT COUNT(*) INTO :claim_before_policy
    FROM   fact_claims  fc
    INNER JOIN fact_policy fp ON fc.policy_id = fp.policy_id
    WHERE  fc.incident_date_key < fp.effective_date_key;

    IF (claim_before_policy > 0) THEN
        INSERT INTO etl_audit_log (batch_id, run_date, script_name, table_name,
            check_type, rows_actual, check_result, error_message)
        VALUES (:batch_id, :run_date, 'etl_04_dq_checks.sql', 'fact_claims',
            'CLAIM_BEFORE_POLICY', :claim_before_policy, 'FAIL',
            'Claims with incident date before policy effective date: '
            || TO_CHAR(:claim_before_policy));
    END IF;

    -- ============================================================
    -- SECTION 6: Final Summary Report + Batch Outcome
    -- ============================================================

    SELECT COUNT(*) INTO :fail_count
    FROM   etl_audit_log
    WHERE  batch_id     = :batch_id
    AND    check_result = 'FAIL';

    IF (fail_count > 0) THEN
        SYSTEM$LOG('ERROR', 'ETL BATCH FAILED: ' || TO_CHAR(:fail_count) || ' data quality checks failed.');
        SYSTEM$LOG('ERROR', 'Review etl_audit_log for batch_id = ' || TO_CHAR(:batch_id));
    ELSE
        SYSTEM$LOG('INFO', 'ETL BATCH PASSED all data quality checks.');
    END IF;

END;


-- #############################################################################
-- OBJECT 2 of 12: etl_audit_log + DQ (enhanced, ETL_Script_04.sql)
-- QUARANTINE CANDIDATE — CURSOR + dynamic SQL + PATINDEX + STUFF + COMPUTE BY
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_04.sql lines 1-479
-- Object : etl_audit_log DDL + comprehensive DQ (enhanced)
-- Series : enhanced
-- Layer  : audit_dq
-- Score  : 11 (Complex, QUARANTINE CANDIDATE)
-- Changes: IDENTITY → AUTOINCREMENT                                    [CONVERT]
--          DATETIME → TIMESTAMP_NTZ                                    [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                             [CONVERT]
--          USER_NAME() → CURRENT_USER()                                [CONVERT]
--          IN DBASPACE → removed                                       [REMOVE]
--          PATINDEX → REGEXP_LIKE                                      [CONVERT]
--          CHARINDEX → POSITION                                        [CONVERT]
--          LEN() → LENGTH()                                            [CONVERT]
--          STUFF(str,1,2,'') → SUBSTR(str,3)                           [CONVERT]
--          STR(num,len) → LPAD(TO_CHAR(num),len)                       [CONVERT]
--          @var = @var + ', ' + col → LISTAGG(col, ', ')               [CONVERT]
--          CURSOR claims_dq → set-based INSERT...SELECT with CASE      [CONVERT]
--          EXEC(@dyn_sql) → EXECUTE IMMEDIATE :dyn_sql                 [CONVERT]
--          COMPUTE BY → GROUP BY ROLLUP()                              [REMOVE]
--          RAISERROR → exception block                                 [CONVERT]
--          CONVERT(VARCHAR, x) → TO_CHAR(x)                            [CONVERT]
--          ISNULL → COALESCE                                           [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- ══════════════════════════════════════════════════════════════
-- TODO: QUARANTINE — This object (score 11) contains CURSOR, dynamic SQL,
-- PATINDEX, STUFF, STR, string accumulation, and COMPUTE BY.
-- All constructs have been converted below but require thorough
-- integration testing due to behavioral differences.
-- Review owner: Data Engineering Lead
-- ══════════════════════════════════════════════════════════════

-- DDL: etl_audit_log (enhanced version)
-- [CONVERT] IF NOT EXISTS ... BEGIN CREATE TABLE → CREATE TABLE IF NOT EXISTS
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] USER_NAME() → CURRENT_USER()
-- [REMOVE] IN DBASPACE
-- [REMOVE] GO
CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id        INT             AUTOINCREMENT NOT NULL,
    script_name     VARCHAR(100)    NOT NULL,
    object_name     VARCHAR(100)    NOT NULL,
    check_type      VARCHAR(50)     NOT NULL,
    status          VARCHAR(20)     NOT NULL,       -- PASS, FAIL, WARNING
    row_count       INT             NULL,
    error_message   VARCHAR(2000)   NULL,
    run_ts          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    run_by          VARCHAR(50)     NOT NULL DEFAULT CURRENT_USER(),
    PRIMARY KEY (audit_id)
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent


-- =============================================================================
-- SECTION 2: Standard DQ Checks (NULL, Duplicate, RI, Date Logic)
-- Combined into a single Snowflake Scripting block
-- =============================================================================

-- [CONVERT] Multiple GO-separated DECLARE blocks → single DECLARE...BEGIN...END block
-- [CONVERT] STR(@fail_count, 6) → LPAD(TO_CHAR(:fail_count), 6)
-- [CONVERT] CONVERT(VARCHAR, @val) → TO_CHAR(:val)
-- [CONVERT] RAISERROR('msg', 10, 1, @p1, @p2) → SYSTEM$LOG (severity 10 = informational)
-- [REMOVE] GO — batch separators
DECLARE
    check_name      VARCHAR(100);
    fail_count      INT;
    check_status    VARCHAR(20);
    dup_count       INT;
    ri_count        INT;
    date_err        INT;
BEGIN

    -- NULL check: critical columns in stg_policies
    check_name := 'STG_POLICIES_NULL_CRITICAL';

    SELECT COUNT(*) INTO :fail_count
    FROM stg_policies
    WHERE policy_id IS NULL
       OR policy_number IS NULL
       OR customer_id IS NULL
       OR agent_id IS NULL
       OR effective_date IS NULL
       OR expiry_date IS NULL
       OR status_code IS NULL;

    check_status := CASE WHEN :fail_count = 0 THEN 'PASS' ELSE 'FAIL' END;

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_policies', :check_name, :check_status, :fail_count,
            CASE WHEN :fail_count > 0
                 -- [CONVERT] STR(@fail_count, 6) → LPAD(TO_CHAR(:fail_count), 6)
                 THEN LPAD(TO_CHAR(:fail_count), 6) || ' rows have NULL in critical columns'
                 ELSE NULL END);

    IF (fail_count > 0) THEN
        -- [CONVERT] RAISERROR severity 10 (informational) → SYSTEM$LOG
        SYSTEM$LOG('WARN', 'DQ FAIL: ' || :check_name || ' — ' || TO_CHAR(:fail_count) || ' rows with NULL critical columns');
    END IF;

    -- Duplicate check: no duplicate policy_id in staging
    SELECT COUNT(*) INTO :dup_count
    FROM (
        SELECT policy_id, COUNT(*) AS cnt
        FROM stg_policies
        GROUP BY policy_id
        HAVING COUNT(*) > 1
    ) dups;

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_policies', 'DUPLICATE_POLICY_ID',
            CASE WHEN :dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
            :dup_count,
            CASE WHEN :dup_count > 0
                 THEN TO_CHAR(:dup_count) || ' duplicate policy_id values in staging'
                 ELSE NULL END);

    -- Referential integrity: all stg_policies.customer_id must exist in stg_customers
    SELECT COUNT(*) INTO :ri_count
    FROM stg_policies p
    WHERE NOT EXISTS (
        SELECT 1 FROM stg_customers c WHERE c.customer_id = p.customer_id
    );

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_policies', 'RI_CUSTOMER_EXISTS',
            CASE WHEN :ri_count = 0 THEN 'PASS' ELSE 'WARNING' END,
            :ri_count,
            CASE WHEN :ri_count > 0
                 THEN TO_CHAR(:ri_count) || ' policies reference non-existent customers'
                 ELSE NULL END);

    -- Date logic check: expiry must be after effective
    SELECT COUNT(*) INTO :date_err
    FROM stg_policies
    WHERE expiry_date <= effective_date;

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_policies', 'DATE_RANGE_VALID',
            CASE WHEN :date_err = 0 THEN 'PASS' ELSE 'FAIL' END,
            :date_err,
            CASE WHEN :date_err > 0
                 THEN TO_CHAR(:date_err) || ' policies have expiry_date <= effective_date'
                 ELSE NULL END);

END;


-- =============================================================================
-- SECTION 3: String Pattern Validation using REGEXP_LIKE and POSITION
-- [CONVERT] PATINDEX → REGEXP_LIKE (inverted logic)
-- [CONVERT] CHARINDEX → POSITION
-- [CONVERT] LEN → LENGTH
-- =============================================================================

DECLARE
    bad_zip         INT;
    bad_email       INT;
    bad_phone       INT;
    bad_routing     INT;
BEGIN

    -- Check: zip_code must match 5-digit or ZIP+4 format
    -- [CONVERT] PATINDEX('[0-9][0-9][0-9][0-9][0-9]', zip_code) = 0 → NOT REGEXP_LIKE(zip_code, '^\d{5}$')
    -- [CONVERT] PATINDEX('[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]', zip_code) = 0 → NOT REGEXP_LIKE(zip_code, '^\d{5}-\d{4}$')
    SELECT COUNT(*) INTO :bad_zip
    FROM stg_customers
    WHERE zip_code IS NOT NULL
      AND NOT REGEXP_LIKE(zip_code, '^\\d{5}$')
      AND NOT REGEXP_LIKE(zip_code, '^\\d{5}-\\d{4}$');

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_customers', 'ZIP_FORMAT_CHECK',
            CASE WHEN :bad_zip = 0 THEN 'PASS' ELSE 'WARNING' END,
            :bad_zip,
            CASE WHEN :bad_zip > 0
                 THEN TO_CHAR(:bad_zip) || ' customers have malformed zip_code'
                 ELSE NULL END);

    -- Check: email must contain @ and a dot after @
    -- [CONVERT] CHARINDEX('@', email) → POSITION('@' IN email)
    -- [CONVERT] CHARINDEX('.', email, CHARINDEX('@', email)) → POSITION('.' IN SUBSTR(email, POSITION('@' IN email)))
    -- [CONVERT] LEN(email) → LENGTH(email)
    SELECT COUNT(*) INTO :bad_email
    FROM stg_customers
    WHERE email IS NOT NULL
      AND (
          POSITION('@' IN email) = 0
          OR POSITION('@' IN email) = LENGTH(email)
          OR POSITION('.' IN SUBSTR(email, POSITION('@' IN email))) = 0
      );

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_customers', 'EMAIL_FORMAT_CHECK',
            CASE WHEN :bad_email = 0 THEN 'PASS' ELSE 'WARNING' END,
            :bad_email,
            CASE WHEN :bad_email > 0
                 THEN TO_CHAR(:bad_email) || ' customers have malformed email addresses'
                 ELSE NULL END);

    -- Check: phone_primary should be 10 digits after stripping separators
    -- [CONVERT] LEN(REPLACE(...)) → LENGTH(REPLACE(...))
    -- [CONVERT] PATINDEX('[0-9]{10}', ...) → REGEXP_LIKE(..., '^\d{10}$')
    SELECT COUNT(*) INTO :bad_phone
    FROM stg_customers
    WHERE phone_primary IS NOT NULL
      AND LENGTH(REPLACE(REPLACE(REPLACE(phone_primary, '-', ''), ' ', ''), '.', '')) <> 10
      AND NOT REGEXP_LIKE(
              REPLACE(REPLACE(REPLACE(phone_primary, '-', ''), ' ', ''), '.', ''),
              '^\\d{10}$');

    -- Check: bank routing number must be exactly 9 digits
    -- [CONVERT] LEN(bank_routing) → LENGTH(bank_routing)
    -- [CONVERT] PATINDEX('[^0-9]', bank_routing) > 0 → NOT REGEXP_LIKE(bank_routing, '^\d{9}$')
    SELECT COUNT(*) INTO :bad_routing
    FROM stg_payments
    WHERE bank_routing IS NOT NULL
      AND (
          LENGTH(bank_routing) <> 9
          OR NOT REGEXP_LIKE(bank_routing, '^\\d+$')
      );

    INSERT INTO etl_audit_log (script_name, object_name, check_type, status, row_count, error_message)
    VALUES ('etl_04_dq', 'stg_payments', 'BANK_ROUTING_FORMAT',
            CASE WHEN :bad_routing = 0 THEN 'PASS' ELSE 'WARNING' END,
            :bad_routing,
            CASE WHEN :bad_routing > 0
                 THEN TO_CHAR(:bad_routing) || ' payments have invalid bank routing numbers'
                 ELSE NULL END);

END;


-- =============================================================================
-- SECTION 4: STUFF/STR → LISTAGG / LPAD(TO_CHAR) for DQ summary
-- [CONVERT] @var = @var + ', ' + col (string accumulation) → LISTAGG(check_type, ', ')
-- [CONVERT] STUFF(@str, 1, 2, '') → not needed (LISTAGG handles delimiters)
-- [CONVERT] STR(num, 4) → LPAD(TO_CHAR(num), 4)
-- [CONVERT] ISNULL(@failed_checks, '') → COALESCE(failed_checks, '')
-- [CONVERT] PRINT → SYSTEM$LOG
-- [CONVERT] RAISERROR → exception
-- [CONVERT] DATEADD(minute, -10, GETDATE()) → DATEADD('MINUTE', -10, CURRENT_TIMESTAMP())
-- =============================================================================

DECLARE
    failed_checks   VARCHAR(2000);
    fail_summary    VARCHAR(4000);
    pass_cnt        INT;
    warn_cnt        INT;
    fail_cnt        INT;
BEGIN

    -- [CONVERT] String accumulation pattern → LISTAGG
    -- Original: SELECT @failed_checks = @failed_checks + ', ' + check_type FROM ...
    -- STUFF(@failed_checks, 1, 2, '') to remove leading ", " is no longer needed
    SELECT LISTAGG(check_type, ', ') WITHIN GROUP (ORDER BY audit_id)
    INTO :failed_checks
    FROM etl_audit_log
    WHERE run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP())
      AND status = 'FAIL';

    -- [CONVERT] STR(num, 4) → LPAD(TO_CHAR(num), 4)
    SELECT COUNT(*) INTO :pass_cnt FROM etl_audit_log
    WHERE run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP()) AND status = 'PASS';

    SELECT COUNT(*) INTO :warn_cnt FROM etl_audit_log
    WHERE run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP()) AND status = 'WARNING';

    SELECT COUNT(*) INTO :fail_cnt FROM etl_audit_log
    WHERE run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP()) AND status = 'FAIL';

    fail_summary := 'DQ Run Summary: '
        || LPAD(TO_CHAR(:pass_cnt), 4)
        || ' passed, '
        || LPAD(TO_CHAR(:warn_cnt), 4)
        || ' warnings, '
        || LPAD(TO_CHAR(:fail_cnt), 4)
        || ' failures.';

    SYSTEM$LOG('INFO', :fail_summary);

    -- [CONVERT] ISNULL(@failed_checks,'') → COALESCE(:failed_checks, '')
    IF (LENGTH(COALESCE(:failed_checks, '')) > 0) THEN
        SYSTEM$LOG('ERROR', 'Failed checks: ' || :failed_checks);
        -- [CONVERT] RAISERROR severity 16 → raise exception
        LET err_msg VARCHAR := 'DQ FAILURES DETECTED: ' || :failed_checks;
        RAISE err_msg;
    END IF;

END;


-- =============================================================================
-- SECTION 5: CURSOR-based Row-Level DQ → SET-BASED rewrite
-- [CONVERT] CURSOR claims_dq_cursor ... WHILE @@FETCH_STATUS = 0 →
--           Single INSERT...SELECT with CASE expressions evaluating all 6 rules
-- [CONVERT] STUFF(@c_error_parts, 1, 1, '') → SUBSTR(error_parts, 2) in CTE
-- [CONVERT] ISNULL(x, 0) → COALESCE(x, 0)
-- [CONVERT] DATEDIFF(day, a, b) → DATEDIFF('DAY', a, b)
-- =============================================================================

-- Set-based rewrite: evaluate all 6 rules per claim row in a single pass
INSERT INTO etl_audit_log (
    script_name, object_name, check_type, status, row_count, error_message
)
WITH claim_rules AS (
    SELECT
        claim_id,
        incident_date,
        reported_date,
        closed_date,
        total_incurred,
        total_paid,
        total_reserved,
        status_code,
        litigation_flag,
        -- Rule 1: reported_date must be >= incident_date
        CASE WHEN reported_date < incident_date
             THEN '|R01:reported<incident' ELSE '' END
        -- Rule 2: closed_date must be >= reported_date (if present)
        || CASE WHEN closed_date IS NOT NULL AND closed_date < reported_date
                THEN '|R02:closed<reported' ELSE '' END
        -- Rule 3: total_paid cannot exceed total_incurred * 1.05 (fraud flag)
        || CASE WHEN COALESCE(total_paid, 0) > COALESCE(total_incurred, 0) * 1.05
                THEN '|R03:paid>incurred' ELSE '' END
        -- Rule 4: closed claims must have a closed_date
        || CASE WHEN status_code = 'CL' AND closed_date IS NULL
                THEN '|R04:closed_no_date' ELSE '' END
        -- Rule 5: litigated claims should have reserve > 0
        || CASE WHEN litigation_flag = 'Y' AND COALESCE(total_reserved, 0) = 0
                THEN '|R05:litigation_no_reserve' ELSE '' END
        -- Rule 6: report lag > 365 days is suspicious
        -- [CONVERT] DATEDIFF(day, a, b) → DATEDIFF('DAY', a, b)
        || CASE WHEN DATEDIFF('DAY', incident_date, reported_date) > 365
                THEN '|R06:lag>365days' ELSE '' END
        AS error_parts
    FROM stg_claims
),
violations AS (
    SELECT
        claim_id,
        -- [CONVERT] STUFF(@c_error_parts, 1, 1, '') → SUBSTR(error_parts, 2)
        SUBSTR(error_parts, 2) AS error_detail,
        error_parts,
        CASE WHEN error_parts LIKE '%R03%' OR error_parts LIKE '%R04%'
             THEN 'FAIL' ELSE 'WARNING' END AS violation_status
    FROM claim_rules
    WHERE LENGTH(error_parts) > 0
)
SELECT
    'etl_04_dq',
    'stg_claims',
    'CLAIM_BUSINESS_RULES',
    violation_status,
    1,
    'Claim ' || TO_CHAR(claim_id) || ': ' || error_detail
FROM violations
ORDER BY claim_id;

-- Summary logging (replaces the cursor epilogue PRINT)
DECLARE
    c_fail_total    INT;
    c_warn_total    INT;
BEGIN
    SELECT COUNT(*) INTO :c_fail_total
    FROM etl_audit_log
    WHERE script_name = 'etl_04_dq'
      AND object_name = 'stg_claims'
      AND check_type  = 'CLAIM_BUSINESS_RULES'
      AND status      = 'FAIL'
      AND run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP());

    SELECT COUNT(*) INTO :c_warn_total
    FROM etl_audit_log
    WHERE script_name = 'etl_04_dq'
      AND object_name = 'stg_claims'
      AND check_type  = 'CLAIM_BUSINESS_RULES'
      AND status      = 'WARNING'
      AND run_ts >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP());

    SYSTEM$LOG('INFO', 'Claims DQ set-based complete. Failures: ' || TO_CHAR(:c_fail_total)
              || ', Warnings: ' || TO_CHAR(:c_warn_total));

    IF (c_fail_total > 0) THEN
        LET err_msg VARCHAR := 'DQ FAIL: ' || TO_CHAR(:c_fail_total) || ' claims failed business rule validation.';
        RAISE err_msg;
    END IF;
END;


-- =============================================================================
-- SECTION 6: Dynamic SQL for Parameterized Row Count Variance Check
-- [CONVERT] EXEC(@dyn_sql) → EXECUTE IMMEDIATE :dyn_sql
-- [CONVERT] CURSOR table_cursor → set-based approach using RESULTSET + loop
-- [CONVERT] OBJECT_ID('tempdb..#row_count_tmp') → CREATE OR REPLACE TEMPORARY TABLE
-- [CONVERT] ISNULL(@prev_count, 0) → COALESCE(:prev_count, 0)
-- =============================================================================

DECLARE
    table_name      VARCHAR(100);
    threshold_pct   NUMBER(5,2);
    curr_count      INT;
    prev_count      INT;
    variance_pct    NUMBER(10,4);
    dyn_sql         VARCHAR(2000);
    rs              RESULTSET;
    c               CURSOR FOR
        SELECT tbl, thresh FROM (
            SELECT 'stg_policies'  AS tbl, 10.0  AS thresh UNION ALL
            SELECT 'stg_customers' AS tbl, 15.0  AS thresh UNION ALL
            SELECT 'stg_claims'    AS tbl, 25.0  AS thresh UNION ALL
            SELECT 'stg_payments'  AS tbl, 30.0  AS thresh
        );
BEGIN
    OPEN c;
    FOR rec IN c DO
        table_name := rec.tbl;
        threshold_pct := rec.thresh;

        -- [CONVERT] Dynamic SQL: EXEC(@dyn_sql) → EXECUTE IMMEDIATE :dyn_sql
        -- Use a temp table intermediary to capture the count
        dyn_sql := 'CREATE OR REPLACE TEMPORARY TABLE _row_count_tmp AS SELECT COUNT(*) AS cnt FROM ' || :table_name;
        EXECUTE IMMEDIATE :dyn_sql;

        SELECT cnt INTO :curr_count FROM _row_count_tmp;

        -- Get previous run count from audit log
        BEGIN
            SELECT row_count INTO :prev_count
            FROM etl_audit_log
            WHERE object_name  = :table_name
              AND check_type   = 'ROW_COUNT_VARIANCE'
              AND status       = 'PASS'
              AND audit_id = (
                  SELECT MAX(audit_id) FROM etl_audit_log
                  WHERE object_name = :table_name
                    AND check_type  = 'ROW_COUNT_VARIANCE'
              );
        EXCEPTION
            WHEN OTHER THEN
                prev_count := NULL;
        END;

        -- Calculate variance
        -- [CONVERT] ISNULL(@prev_count, 0) → COALESCE(:prev_count, 0)
        IF (COALESCE(:prev_count, 0) > 0) THEN
            variance_pct := ABS(CAST(:curr_count - :prev_count AS NUMBER(18,4))
                                / :prev_count * 100.0);

            INSERT INTO etl_audit_log (
                script_name, object_name, check_type, status, row_count, error_message
            )
            VALUES (
                'etl_04_dq', :table_name, 'ROW_COUNT_VARIANCE',
                CASE WHEN :variance_pct > :threshold_pct THEN 'WARNING' ELSE 'PASS' END,
                :curr_count,
                'Variance: ' || TO_CHAR(ROUND(:variance_pct, 2))
                || '% (threshold: ' || TO_CHAR(:threshold_pct) || '%)'
                || ' Prev: ' || TO_CHAR(:prev_count)
                || ' Curr: ' || TO_CHAR(:curr_count)
            );
        ELSE
            -- First run — baseline
            INSERT INTO etl_audit_log (
                script_name, object_name, check_type, status, row_count, error_message
            )
            VALUES ('etl_04_dq', :table_name, 'ROW_COUNT_VARIANCE', 'PASS',
                    :curr_count, 'First run — baseline established');
        END IF;
    END FOR;
    CLOSE c;

    -- [TODO] SQL injection risk: table_name is concatenated into dynamic SQL.
    -- In production, validate table_name against an allowlist before EXECUTE IMMEDIATE.

END;


-- =============================================================================
-- SECTION 7: COMPUTE BY → GROUP BY ROLLUP()
-- [REMOVE] COMPUTE SUM(...) BY check_type → replaced with GROUP BY ROLLUP
-- [CONVERT] ISNULL(row_count, 0) → COALESCE(row_count, 0)
-- [CONVERT] DATEADD(hour, -1, GETDATE()) → DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
-- =============================================================================

-- [REMOVE] Original: COMPUTE SUM(COUNT(*)), SUM(SUM(ISNULL(row_count, 0))) BY check_type
-- Replaced with GROUP BY ROLLUP to produce subtotals per check_type and grand total
SELECT
    check_type,
    status,
    COUNT(*)                        AS check_count,
    SUM(COALESCE(row_count, 0))     AS total_rows_flagged
FROM etl_audit_log
WHERE run_ts >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY ROLLUP(check_type, status)
ORDER BY check_type NULLS LAST, status NULLS LAST;

-- Overall totals by status
-- [REMOVE] Original: COMPUTE SUM(COUNT(*)), SUM(SUM(ISNULL(row_count, 0)))
-- Replaced with GROUP BY ROLLUP for grand total row
SELECT
    status,
    COUNT(*)                        AS check_count,
    SUM(COALESCE(row_count, 0))     AS total_flagged_rows
FROM etl_audit_log
WHERE run_ts >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY ROLLUP(status)
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARNING' THEN 2 WHEN 'PASS' THEN 3 ELSE 4 END;


-- #############################################################################
-- OBJECT 3 of 12: agg_policy_monthly (enhanced, ETL_Script_05.sql)
-- HIGH QUARANTINE RISK — Correlated subqueries + WHILE gap-fill + COMPUTE BY
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_05.sql lines 1-207
-- Object : agg_policy_monthly (TABLE + INSERT + rolling calcs + gap-fill)
-- Series : enhanced
-- Layer  : aggregate
-- Score  : 16 (Complex, HIGH QUARANTINE RISK)
-- Changes: IF EXISTS/DROP → CREATE OR REPLACE                          [CONVERT]
--          IDENTITY → AUTOINCREMENT                                    [CONVERT]
--          TINYINT → SMALLINT                                          [CONVERT]
--          DECIMAL(n,m) → NUMBER(n,m)                                  [CONVERT]
--          IN DBASPACE → removed                                       [REMOVE]
--          ISNULL(x,y) → COALESCE(x,y)                                [CONVERT]
--          Correlated subquery (rolling 12m) → window SUM() OVER()    [CONVERT]
--          Correlated subquery (YoY) → LAG() window function          [CONVERT]
--          WHILE gap-fill loop → GENERATOR() + CROSS JOIN             [CONVERT]
--          CONVERT(VARCHAR, @val) → TO_CHAR(:val)                      [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- ══════════════════════════════════════════════════════════════
-- TODO: QUARANTINE — This object (score 16) rewrites correlated subqueries
-- as window functions and replaces the WHILE gap-fill loop with
-- GENERATOR(). Verify rolling 12-month premium and YoY calculations
-- produce identical results to the Sybase version.
-- Review owner: Data Engineering Lead
-- ══════════════════════════════════════════════════════════════

-- [CONVERT] IF EXISTS ... DROP TABLE → CREATE OR REPLACE TABLE
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] DECIMAL(n,m) → NUMBER(n,m)
-- [REMOVE] IN DBASPACE
-- [REMOVE] GO
CREATE OR REPLACE TABLE agg_policy_monthly (
    agg_key                 INT             AUTOINCREMENT NOT NULL,
    year_number             SMALLINT        NOT NULL,
    quarter_number          SMALLINT        NOT NULL,       -- [CONVERT] TINYINT → SMALLINT
    month_number            SMALLINT        NOT NULL,       -- [CONVERT] TINYINT → SMALLINT
    product_line            VARCHAR(50)     NOT NULL,
    lob_code                VARCHAR(10)     NOT NULL,
    state_code              CHAR(2)         NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    channel_code            VARCHAR(20)     NULL,
    policy_count            INT             NULL,
    new_policy_count        INT             NULL,
    renewal_count           INT             NULL,
    cancellation_count      INT             NULL,
    total_annual_premium    NUMBER(18,2)    NULL,
    total_earned_premium    NUMBER(18,2)    NULL,
    total_coverage_amount   NUMBER(18,2)    NULL,
    avg_premium             NUMBER(18,2)    NULL,
    running_12m_premium     NUMBER(18,2)    NULL,           -- Populated via window function
    yoy_premium_chg_pct     NUMBER(10,4)    NULL,           -- Populated via LAG()
    PRIMARY KEY (agg_key)
);

-- Base INSERT: populate from fact + dimension joins
-- [CONVERT] ISNULL(x, 'val') → COALESCE(x, 'val')
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
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code, 'UNKNOWN'),
    COALESCE(dc.state_code, 'XX'),
    COALESCE(dc.region, 'UNKNOWN'),
    COALESCE(da.channel_code, 'UNKNOWN'),
    COUNT(DISTINCT fp.policy_id),
    COUNT(DISTINCT CASE WHEN fp.renewal_count = 0 THEN fp.policy_id ELSE NULL END),
    COUNT(DISTINCT CASE WHEN fp.renewal_count > 0
                             AND fp.status_code = 'RN' THEN fp.policy_id ELSE NULL END),
    COUNT(DISTINCT CASE WHEN fp.status_code = 'CN' THEN fp.policy_id ELSE NULL END),
    ROUND(SUM(COALESCE(fp.annual_premium, 0)), 2),
    ROUND(SUM(COALESCE(fp.earned_premium_amount, 0)), 2),
    ROUND(SUM(COALESCE(fp.coverage_amount, 0)), 2),
    CASE WHEN COUNT(fp.policy_id) > 0
         THEN ROUND(SUM(COALESCE(fp.annual_premium, 0)) / COUNT(fp.policy_id), 2)
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
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code, 'UNKNOWN'),
    COALESCE(dc.state_code, 'XX'),
    COALESCE(dc.region, 'UNKNOWN'),
    COALESCE(da.channel_code, 'UNKNOWN');


-- =============================================================================
-- SECTION 2: Rolling 12-Month Premium — Window Function Rewrite
-- [CONVERT] Correlated subquery → SUM() OVER (PARTITION BY ... ORDER BY ...
--           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
-- =============================================================================

UPDATE agg_policy_monthly tgt
SET running_12m_premium = src.running_12m_premium
FROM (
    SELECT
        agg_key,
        SUM(total_annual_premium) OVER (
            PARTITION BY product_line, lob_code, state_code
            ORDER BY year_number, month_number
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS running_12m_premium
    FROM agg_policy_monthly
) src
WHERE tgt.agg_key = src.agg_key;


-- =============================================================================
-- SECTION 3: Year-over-Year Premium Change — LAG() Rewrite
-- [CONVERT] Correlated subquery for prior year → LAG(total_annual_premium, 12)
-- =============================================================================

UPDATE agg_policy_monthly tgt
SET yoy_premium_chg_pct = src.yoy_premium_chg_pct
FROM (
    SELECT
        agg_key,
        CASE WHEN COALESCE(LAG(total_annual_premium, 12) OVER (
                    PARTITION BY product_line, lob_code, state_code
                    ORDER BY year_number, month_number
                  ), 0) > 0
             THEN ROUND(
                (total_annual_premium - LAG(total_annual_premium, 12) OVER (
                    PARTITION BY product_line, lob_code, state_code
                    ORDER BY year_number, month_number
                ))
                / LAG(total_annual_premium, 12) OVER (
                    PARTITION BY product_line, lob_code, state_code
                    ORDER BY year_number, month_number
                ) * 100.0, 4)
             ELSE NULL END AS yoy_premium_chg_pct
    FROM agg_policy_monthly
) src
WHERE tgt.agg_key = src.agg_key;


-- =============================================================================
-- SECTION 4: Gap-Fill for Missing Months — GENERATOR() Rewrite
-- [CONVERT] WHILE @gap_year <= @gap_max_year ... INSERT missing combos →
--           GENERATOR() cross join to distinct dimension combos with NOT EXISTS
-- =============================================================================

DECLARE
    gap_min_year    SMALLINT;
    gap_max_year    SMALLINT;
    total_months    INT;
BEGIN
    SELECT MIN(year_number) INTO :gap_min_year FROM agg_policy_monthly;
    SELECT MAX(year_number) INTO :gap_max_year FROM agg_policy_monthly;

    total_months := (:gap_max_year - :gap_min_year + 1) * 12;

    INSERT INTO agg_policy_monthly (
        year_number, quarter_number, month_number,
        product_line, lob_code, state_code, region, channel_code,
        policy_count, new_policy_count, renewal_count, cancellation_count,
        total_annual_premium, total_earned_premium, total_coverage_amount, avg_premium,
        running_12m_premium, yoy_premium_chg_pct
    )
    WITH month_spine AS (
        SELECT
            :gap_min_year + FLOOR((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) / 12) AS year_number,
            ((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) % 12) + 1 AS month_number,
            CASE
                WHEN ((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) % 12) + 1 BETWEEN 1 AND 3  THEN 1
                WHEN ((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) % 12) + 1 BETWEEN 4 AND 6  THEN 2
                WHEN ((ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) % 12) + 1 BETWEEN 7 AND 9  THEN 3
                ELSE 4
            END AS quarter_number
        FROM TABLE(GENERATOR(ROWCOUNT => :total_months))
    ),
    combos AS (
        SELECT DISTINCT product_line, lob_code, state_code, region, channel_code
        FROM agg_policy_monthly
    )
    SELECT
        ms.year_number,
        ms.quarter_number,
        ms.month_number,
        c.product_line,
        c.lob_code,
        c.state_code,
        c.region,
        c.channel_code,
        0, 0, 0, 0,
        0.00, 0.00, 0.00, NULL,
        NULL, NULL
    FROM month_spine ms
    CROSS JOIN combos c
    WHERE NOT EXISTS (
        SELECT 1 FROM agg_policy_monthly a
        WHERE a.year_number   = ms.year_number
          AND a.month_number  = ms.month_number
          AND a.product_line  = c.product_line
          AND a.lob_code      = c.lob_code
          AND a.state_code    = c.state_code
    );

    SYSTEM$LOG('INFO', 'Gap-fill complete via GENERATOR cross join');
END;


-- #############################################################################
-- OBJECT 4 of 12: agg_claims_monthly (enhanced, ETL_Script_05.sql)
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_05.sql lines 209-319
-- Object : agg_claims_monthly (TABLE + INSERT + COMPUTE BY + loss ratio UPDATE)
-- Series : enhanced
-- Layer  : aggregate
-- Score  : 8 (Complex)
-- Changes: IF EXISTS/DROP → CREATE OR REPLACE                          [CONVERT]
--          IDENTITY → AUTOINCREMENT                                    [CONVERT]
--          TINYINT → SMALLINT                                          [CONVERT]
--          DECIMAL(n,m) → NUMBER(n,m)                                  [CONVERT]
--          IN DBASPACE → removed                                       [REMOVE]
--          ISNULL(x,y) → COALESCE(x,y)                                [CONVERT]
--          COMPUTE BY → GROUP BY ROLLUP()                              [REMOVE]
--          Sybase UPDATE...FROM → Snowflake UPDATE...FROM              [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- [CONVERT] IF EXISTS ... DROP TABLE → CREATE OR REPLACE TABLE
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] DECIMAL(n,m) → NUMBER(n,m)
-- [REMOVE] IN DBASPACE
-- [REMOVE] GO
CREATE OR REPLACE TABLE agg_claims_monthly (
    agg_key             INT             AUTOINCREMENT NOT NULL,
    year_number         SMALLINT        NOT NULL,
    quarter_number      SMALLINT        NOT NULL,       -- [CONVERT] TINYINT → SMALLINT
    month_number        SMALLINT        NOT NULL,       -- [CONVERT] TINYINT → SMALLINT
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
    total_incurred      NUMBER(18,2)    NULL,
    total_paid          NUMBER(18,2)    NULL,
    total_reserved      NUMBER(18,2)    NULL,
    total_outstanding   NUMBER(18,2)    NULL,
    total_salvage       NUMBER(18,2)    NULL,
    total_subrogation   NUMBER(18,2)    NULL,
    avg_incurred        NUMBER(18,2)    NULL,
    avg_settlement_days NUMBER(10,2)    NULL,
    avg_report_lag_days NUMBER(10,2)    NULL,
    earned_premium_ref  NUMBER(18,2)    NULL,
    loss_ratio_pct      NUMBER(10,4)    NULL,
    PRIMARY KEY (agg_key)
);
-- [REMOVE] IN DBASPACE — Sybase IQ storage directive; no Snowflake equivalent

-- Base INSERT
-- [CONVERT] ISNULL(x, y) → COALESCE(x, y)
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
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code, 'UNKNOWN'),
    COALESCE(dc.state_code, 'XX'),
    fc.claim_type_code,
    fc.status_code,
    COUNT(fc.claim_id),
    COUNT(CASE WHEN fc.status_code NOT IN ('CL','WD') THEN 1 END),
    COUNT(CASE WHEN fc.status_code IN ('CL','WD') THEN 1 END),
    COUNT(CASE WHEN fc.litigation_flag = 'Y' THEN 1 END),
    COUNT(CASE WHEN fc.catastrophe_code IS NOT NULL THEN 1 END),
    ROUND(SUM(COALESCE(fc.total_incurred, 0)), 2),
    ROUND(SUM(COALESCE(fc.total_paid, 0)), 2),
    ROUND(SUM(COALESCE(fc.total_reserved, 0)), 2),
    ROUND(SUM(COALESCE(fc.outstanding_reserve, 0)), 2),
    ROUND(SUM(COALESCE(fc.salvage_amount, 0)), 2),
    ROUND(SUM(COALESCE(fc.subrogation_amount, 0)), 2),
    ROUND(AVG(CAST(COALESCE(fc.total_incurred, 0) AS NUMBER(18,4))), 2),
    ROUND(AVG(CAST(COALESCE(fc.claim_settlement_days, 0) AS NUMBER(10,2))), 2),
    ROUND(AVG(CAST(COALESCE(fc.claim_report_lag_days, 0) AS NUMBER(10,2))), 2)
FROM fact_claims fc
INNER JOIN dim_date     dd ON dd.date_key     = fc.incident_date_key
INNER JOIN fact_policy  fp ON fp.policy_id    = fc.policy_id
INNER JOIN dim_product  dp ON dp.product_key  = fp.product_key
INNER JOIN dim_customer dc ON dc.customer_key = fc.customer_key
WHERE dd.year_number >= 2015
GROUP BY
    dd.year_number, dd.quarter_number, dd.month_number,
    COALESCE(dp.product_line, 'UNKNOWN'),
    COALESCE(dp.lob_code, 'UNKNOWN'),
    COALESCE(dc.state_code, 'XX'),
    fc.claim_type_code,
    fc.status_code;

-- [REMOVE] COMPUTE BY nested subtotals:
-- Original: COMPUTE SUM(SUM(claim_count)), SUM(SUM(total_incurred)) BY year_number, product_line
-- Original: COMPUTE SUM(SUM(claim_count)), SUM(SUM(total_incurred)) BY year_number
-- Replaced with GROUP BY ROLLUP for equivalent subtotals
SELECT
    year_number, product_line, lob_code,
    SUM(claim_count)    AS claims,
    SUM(total_incurred) AS incurred,
    SUM(total_paid)     AS paid
FROM agg_claims_monthly
WHERE year_number >= 2020
GROUP BY ROLLUP(year_number, product_line, lob_code)
ORDER BY year_number NULLS LAST, product_line NULLS LAST, lob_code NULLS LAST;

-- Update loss ratio
-- [CONVERT] Sybase UPDATE...FROM with self-alias → Snowflake UPDATE...FROM
UPDATE agg_claims_monthly acm
SET
    earned_premium_ref = apm.total_earned_premium,
    loss_ratio_pct = CASE WHEN apm.total_earned_premium > 0
        THEN ROUND(acm.total_incurred / apm.total_earned_premium * 100.0, 4)
        ELSE NULL END
FROM agg_policy_monthly apm
WHERE apm.year_number   = acm.year_number
  AND apm.month_number  = acm.month_number
  AND apm.lob_code      = acm.lob_code
  AND apm.state_code    = acm.state_code;


-- #############################################################################
-- OBJECT 5 of 12: v_exec_kpi_summary (enhanced, ETL_Script_05.sql)
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_05.sql lines 322-370
-- Object : v_exec_kpi_summary (VIEW)
-- Series : enhanced
-- Layer  : view
-- Score  : 7 (Complex)
-- Changes: IF EXISTS/DROP VIEW → CREATE OR REPLACE VIEW                [CONVERT]
--          ISNULL(x,y) → COALESCE(x,y)                                [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- [CONVERT] IF EXISTS ... DROP VIEW → CREATE OR REPLACE VIEW
-- [CONVERT] ISNULL → COALESCE
-- [REMOVE] GO
CREATE OR REPLACE VIEW v_exec_kpi_summary AS
SELECT
    apm.year_number,
    apm.quarter_number,
    apm.month_number,
    apm.product_line,
    apm.lob_code,
    apm.region,
    apm.state_code,
    SUM(apm.policy_count)                   AS total_policies,
    SUM(apm.new_policy_count)               AS new_policies,
    SUM(apm.renewal_count)                  AS renewals,
    SUM(apm.cancellation_count)             AS cancellations,
    SUM(apm.total_annual_premium)           AS gwp,
    SUM(apm.total_earned_premium)           AS earned_premium,
    MAX(apm.running_12m_premium)            AS rolling_12m_gwp,
    SUM(COALESCE(acm.claim_count, 0))       AS claim_count,
    SUM(COALESCE(acm.total_incurred, 0))    AS total_losses,
    SUM(COALESCE(acm.total_outstanding, 0)) AS ibnr_reserve,
    CASE WHEN SUM(apm.total_earned_premium) > 0
         THEN ROUND(SUM(COALESCE(acm.total_incurred, 0))
                     / SUM(apm.total_earned_premium) * 100, 2)
         ELSE NULL END                      AS loss_ratio_pct,
    CASE WHEN SUM(apm.policy_count) > 0
         THEN ROUND(SUM(COALESCE(acm.claim_count, 0)) * 1000.0
                     / SUM(apm.policy_count), 4)
         ELSE NULL END                      AS claim_freq_per_1000,
    CASE WHEN SUM(COALESCE(acm.claim_count, 0)) > 0
         THEN ROUND(SUM(COALESCE(acm.total_incurred, 0))
                     / SUM(acm.claim_count), 2)
         ELSE NULL END                      AS avg_severity,
    AVG(apm.yoy_premium_chg_pct)            AS avg_yoy_premium_growth
FROM agg_policy_monthly apm
LEFT JOIN agg_claims_monthly acm
    ON acm.year_number   = apm.year_number
   AND acm.month_number  = apm.month_number
   AND acm.lob_code      = apm.lob_code
   AND acm.state_code    = apm.state_code
GROUP BY
    apm.year_number, apm.quarter_number, apm.month_number,
    apm.product_line, apm.lob_code, apm.region, apm.state_code;


-- #############################################################################
-- OBJECT 6 of 12: trg_kpi_view_readonly (ETL_Script_05.sql)
-- QUARANTINE CANDIDATE — INSTEAD OF trigger, no Snowflake equivalent
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_05.sql lines 375-391
-- Object : trg_kpi_view_readonly (INSTEAD OF TRIGGER)
-- Series : enhanced
-- Layer  : trigger
-- Score  : 3 (Simple, QUARANTINE CANDIDATE)
-- =============================================================================

-- ══════════════════════════════════════════════════════════════
-- TODO: MANUAL CONVERSION REQUIRED
-- Construct: INSTEAD_OF_TRIGGER
-- Source: ETL_Script_05.sql:375-391
-- Reason: Snowflake does not support INSTEAD OF triggers on views.
-- Suggested approach: Enforce read-only access via RBAC (GRANT SELECT only).
--   Alternatively, implement application-layer validation.
--   Example RBAC implementation:
--     GRANT SELECT ON VIEW v_exec_kpi_summary TO ROLE reporting_readonly;
--     -- Do NOT grant INSERT, UPDATE, or DELETE on the view.
--     -- Views in Snowflake are not updatable by default, so no trigger is needed.
-- Review owner: Data Engineering Lead
-- ══════════════════════════════════════════════════════════════

-- [REMOVE] Original Sybase source (preserved as comment for reference):
--   CREATE TRIGGER trg_kpi_view_readonly
--   ON v_exec_kpi_summary
--   INSTEAD OF UPDATE, DELETE
--   AS
--   BEGIN
--       RAISERROR (
--           'v_exec_kpi_summary is read-only. Update the underlying aggregate tables directly.',
--           16, 1
--       )
--       ROLLBACK TRANSACTION
--   END


-- #############################################################################
-- OBJECT 7 of 12: trg_agg_policy_audit (ETL_Script_05.sql)
-- AFTER UPDATE trigger → Snowflake Stream + Task pattern
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_05.sql lines 393-413
-- Object : trg_agg_policy_audit (AFTER UPDATE TRIGGER)
-- Series : enhanced
-- Layer  : trigger
-- Score  : 4 (Medium)
-- Changes: AFTER UPDATE trigger → Stream + Task pattern                [TODO]
--          USER_NAME() → CURRENT_USER()                               [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                            [CONVERT]
-- =============================================================================

-- ══════════════════════════════════════════════════════════════
-- TODO: AFTER UPDATE trigger → Snowflake Stream + Task pattern
-- Source: ETL_Script_05.sql:393-413
-- Reason: Snowflake does not support triggers. Use a Stream on
--   agg_policy_monthly to capture changes, then a scheduled Task
--   to INSERT audit rows into etl_audit_log.
--
-- Suggested implementation:
-- ══════════════════════════════════════════════════════════════

-- Step 1: Create a stream on the source table to capture UPDATE changes
CREATE OR REPLACE STREAM stream_agg_policy_audit
    ON TABLE agg_policy_monthly
    APPEND_ONLY = FALSE;
    -- APPEND_ONLY = FALSE captures INSERT, UPDATE, and DELETE

-- Step 2: Create a task that processes the stream and inserts audit rows
-- [CONVERT] USER_NAME() → CURRENT_USER()
CREATE OR REPLACE TASK task_agg_policy_audit
    WAREHOUSE = 'ETL_WH'           -- [TODO] Set to your ETL warehouse name
    SCHEDULE  = '1 MINUTE'         -- [TODO] Adjust frequency as needed
    WHEN SYSTEM$STREAM_HAS_DATA('stream_agg_policy_audit')
AS
    INSERT INTO etl_audit_log (
        script_name, object_name, check_type, status, row_count, error_message
    )
    SELECT
        'STREAM_TASK',
        'agg_policy_monthly',
        'AGG_UPDATE',
        'PASS',
        COUNT(*),
        'Aggregate updated by: ' || CURRENT_USER()
    FROM stream_agg_policy_audit
    WHERE METADATA$ACTION = 'INSERT'
      AND METADATA$ISUPDATE = TRUE;

-- [TODO] Enable the task after validation:
-- ALTER TASK task_agg_policy_audit RESUME;


-- #############################################################################
-- OBJECT 8 of 12: sp_validate_claims (ETL_Script_06.sql)
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_06.sql lines 1-73
-- Object : sp_validate_claims (STORED PROCEDURE)
-- Series : enhanced
-- Layer  : procedure
-- Score  : 7 (Complex)
-- Changes: CREATE PROCEDURE ... AS BEGIN → CREATE OR REPLACE PROCEDURE
--            ... RETURNS TABLE(...) LANGUAGE SQL AS DECLARE ... BEGIN   [CONVERT]
--          TINYINT param → SMALLINT                                    [CONVERT]
--          @batch_date → P_BATCH_DATE                                  [CONVERT]
--          @include_invalid → P_INCLUDE_INVALID                        [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                             [CONVERT]
--          ISNULL(x,y) → COALESCE(x,y)                                [CONVERT]
--          IF @@ERROR <> 0 → EXCEPTION WHEN OTHER THEN                [CONVERT]
--          RAISERROR → RAISE                                           [CONVERT]
--          RETURN -1 → RAISE exception                                 [CONVERT]
--          SELECT result set → RETURN TABLE(...)                       [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- [CONVERT] Sybase stored procedure returning result set → Snowflake procedure RETURNS TABLE(...)
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] @@ERROR → EXCEPTION WHEN OTHER THEN
CREATE OR REPLACE PROCEDURE sp_validate_claims(
    P_BATCH_DATE        DATE        DEFAULT NULL,
    P_INCLUDE_INVALID   SMALLINT    DEFAULT 0
)
RETURNS TABLE (
    claim_id            BIGINT,
    claim_number        VARCHAR(30),
    policy_id           BIGINT,
    customer_id         BIGINT,
    incident_date       DATE,
    reported_date       DATE,
    closed_date         DATE,
    claim_type_code     VARCHAR(20),
    sub_type_code       VARCHAR(20),
    status_code         VARCHAR(10),
    fault_indicator     CHAR(1),
    litigation_flag     CHAR(1),
    catastrophe_code    VARCHAR(10),
    reinsurance_flag    CHAR(1),
    total_incurred      NUMBER(18,2),
    total_paid          NUMBER(18,2),
    total_reserved      NUMBER(18,2),
    salvage_amount      NUMBER(18,2),
    subrogation_amount  NUMBER(18,2),
    validation_status   VARCHAR(10)
)
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_date DATE;
BEGIN
    -- Apply default batch date if not provided
    -- [CONVERT] GETDATE() → CURRENT_DATE()
    IF (:P_BATCH_DATE IS NULL) THEN
        v_batch_date := CURRENT_DATE();
    ELSE
        v_batch_date := :P_BATCH_DATE;
    END IF;

    -- Return validated claims
    -- [CONVERT] ISNULL(x, 0) → COALESCE(x, 0)
    -- [CONVERT] ISNULL(sc.reported_date, GETDATE()) → COALESCE(sc.reported_date, CURRENT_DATE())
    RETURN TABLE(
        SELECT
            sc.claim_id,
            sc.claim_number,
            sc.policy_id,
            sc.customer_id,
            sc.incident_date,
            sc.reported_date,
            sc.closed_date,
            sc.claim_type_code,
            sc.sub_type_code,
            sc.status_code,
            sc.fault_indicator,
            sc.litigation_flag,
            sc.catastrophe_code,
            sc.reinsurance_flag,
            -- Apply floor to negative amounts (data quality guard)
            CASE WHEN COALESCE(sc.total_incurred, 0) < 0 THEN 0.00 ELSE sc.total_incurred END,
            CASE WHEN COALESCE(sc.total_paid, 0)     < 0 THEN 0.00 ELSE sc.total_paid     END,
            CASE WHEN COALESCE(sc.total_reserved, 0) < 0 THEN 0.00 ELSE sc.total_reserved END,
            COALESCE(sc.salvage_amount, 0.00),
            COALESCE(sc.subrogation_amount, 0.00),
            CASE
                WHEN sc.policy_id IS NOT NULL
                 AND sc.incident_date <= COALESCE(sc.reported_date, CURRENT_DATE())
                THEN 'VALID'
                ELSE 'INVALID'
            END AS validation_status
        FROM stg_claims sc
        WHERE
            sc.claim_id IS NOT NULL
            AND sc.policy_id IS NOT NULL
            AND (
                :P_INCLUDE_INVALID = 1
                OR (
                    sc.policy_id IS NOT NULL
                    AND sc.incident_date <= COALESCE(sc.reported_date, CURRENT_DATE())
                )
            )
    );

EXCEPTION
    -- [CONVERT] IF @@ERROR <> 0 BEGIN RAISERROR ... RETURN -1 END → EXCEPTION block
    WHEN OTHER THEN
        LET err_msg VARCHAR := 'sp_validate_claims: Query failed. ' || SQLERRM;
        RAISE err_msg;
END;
$$


-- #############################################################################
-- OBJECT 9 of 12: sp_run_etl_pipeline (ETL_Script_06.sql)
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_06.sql lines 75-151
-- Object : sp_run_etl_pipeline (STORED PROCEDURE — orchestrator)
-- Series : enhanced
-- Layer  : procedure
-- Score  : 9 (Complex)
-- Changes: CREATE PROCEDURE ... AS BEGIN → CREATE OR REPLACE PROCEDURE [CONVERT]
--          TINYINT → SMALLINT                                          [CONVERT]
--          EXEC sp_validate_claims @p=v → CALL sp_validate_claims(v)  [CONVERT]
--          @@ERROR → EXCEPTION / try-catch pattern                    [CONVERT]
--          PRINT → SYSTEM$LOG                                          [CONVERT]
--          CONVERT(VARCHAR, date, 120) → TO_CHAR(date, 'YYYY-MM-DD HH24:MI:SS') [CONVERT]
--          CONVERT(VARCHAR, date, 112) → TO_CHAR(date, 'YYYYMMDD')     [CONVERT]
--          RAISERROR → RAISE                                           [CONVERT]
--          RETURN @total_fails → RETURN total_fails                    [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- [CONVERT] Sybase orchestrator procedure → Snowflake Scripting procedure
CREATE OR REPLACE PROCEDURE sp_run_etl_pipeline(
    P_RUN_DATE       DATE        DEFAULT NULL,
    P_RUN_MODE       VARCHAR(10) DEFAULT 'FULL',        -- FULL or INCREMENTAL
    P_ABORT_ON_FAIL  SMALLINT    DEFAULT 1              -- [CONVERT] TINYINT → SMALLINT
)
RETURNS INT
LANGUAGE SQL
AS
$$
DECLARE
    step_name       VARCHAR(100);
    step_start      TIMESTAMP_NTZ;
    step_rc         INT;
    total_fails     INT := 0;
    v_run_date      DATE;
    stg_count       INT;
    err_msg         VARCHAR(2000);
BEGIN
    -- [CONVERT] GETDATE() → CURRENT_DATE()
    IF (:P_RUN_DATE IS NULL) THEN
        v_run_date := CURRENT_DATE();
    ELSE
        v_run_date := :P_RUN_DATE;
    END IF;

    -- [CONVERT] PRINT → SYSTEM$LOG
    -- [CONVERT] CONVERT(VARCHAR, GETDATE(), 120) → TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')
    SYSTEM$LOG('INFO', '========================================================');
    SYSTEM$LOG('INFO', 'ETL Pipeline Start: ' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS'));
    SYSTEM$LOG('INFO', 'Mode: ' || :P_RUN_MODE || ' | Date: ' || TO_CHAR(:v_run_date, 'YYYYMMDD'));
    SYSTEM$LOG('INFO', '========================================================');

    -- Step 1: Validate staging is populated
    step_name  := 'STAGING_ROW_COUNT_CHECK';
    step_start := CURRENT_TIMESTAMP();

    SELECT COUNT(*) INTO :stg_count FROM stg_policies;
    IF (stg_count = 0) THEN
        INSERT INTO etl_audit_log (script_name, object_name, check_type, status, error_message)
        VALUES ('sp_run_etl_pipeline', 'stg_policies', :step_name, 'FAIL',
                'stg_policies is empty — staging not loaded');
        IF (:P_ABORT_ON_FAIL = 1) THEN
            err_msg := 'Pipeline aborted: staging tables are empty.';
            RAISE err_msg;
        END IF;
        total_fails := :total_fails + 1;
    END IF;

    -- Step 2: Load dimensions (validate claims as example)
    step_name  := 'LOAD_DIMENSIONS';
    step_start := CURRENT_TIMESTAMP();
    SYSTEM$LOG('INFO', 'Step: ' || :step_name || ' | ' || TO_CHAR(:step_start, 'YYYY-MM-DD HH24:MI:SS'));

    -- [CONVERT] EXEC sp_validate_claims @batch_date = @run_date, @include_invalid = 0
    --         → CALL sp_validate_claims(:v_run_date, 0) wrapped in try-catch
    BEGIN
        CALL sp_validate_claims(:v_run_date, 0);
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO etl_audit_log (script_name, object_name, check_type, status, error_message)
            VALUES ('sp_run_etl_pipeline', 'DIMENSIONS', :step_name, 'FAIL',
                    'Dimension load failed with error: ' || SQLERRM);
            total_fails := :total_fails + 1;
            IF (:P_ABORT_ON_FAIL = 1) THEN
                err_msg := 'Pipeline aborted at step: ' || :step_name;
                RAISE err_msg;
            END IF;
    END;

    -- Final summary
    SYSTEM$LOG('INFO', '========================================================');
    SYSTEM$LOG('INFO', 'ETL Pipeline End:   ' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS'));
    SYSTEM$LOG('INFO', 'Total Failures:     ' || TO_CHAR(:total_fails));
    SYSTEM$LOG('INFO', '========================================================');

    RETURN :total_fails;
END;
$$

-- #############################################################################
-- OBJECT 10 of 12: sp_archive_stale_policies (ETL_Script_06.sql)
-- QUARANTINE CANDIDATE — CURSOR + GOTO + per-row transactions
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_06.sql lines 153-249
-- Object : sp_archive_stale_policies (STORED PROCEDURE)
-- Series : enhanced
-- Layer  : procedure
-- Score  : 8 (Complex, QUARANTINE CANDIDATE)
-- Changes: CURSOR + per-row INSERT/DELETE → set-based INSERT...SELECT + DELETE [CONVERT]
--          GOTO next_policy → restructured as IF/ELSE + CONTINUE       [CONVERT]
--          BEGIN TRAN/COMMIT/ROLLBACK → single atomic DML (set-based) [CONVERT]
--          TINYINT → SMALLINT                                          [CONVERT]
--          @@ERROR → EXCEPTION                                         [CONVERT]
--          PRINT → SYSTEM$LOG                                          [CONVERT]
--          CONVERT(VARCHAR, x) → TO_CHAR(x)                            [CONVERT]
--          CONVERT(VARCHAR, date, 112) → TO_CHAR(date, 'YYYYMMDD')     [CONVERT]
--          DATEADD(year, -n, GETDATE()) → DATEADD('YEAR', -n, CURRENT_DATE()) [CONVERT]
--          GETDATE() → CURRENT_TIMESTAMP()                             [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- ══════════════════════════════════════════════════════════════
-- TODO: QUARANTINE — This object (score 8) completely replaces the CURSOR +
-- GOTO + per-row transaction pattern with set-based DML. The original
-- archived one policy at a time with individual transactions. The
-- Snowflake version uses a single INSERT...SELECT + DELETE which is
-- atomic. Verify that the batch_size limit is respected if needed.
-- The dry_run mode now uses a SELECT preview instead of per-row PRINT.
-- Review owner: Data Engineering Lead
-- ══════════════════════════════════════════════════════════════

-- [CONVERT] CURSOR + GOTO + per-row transactions → set-based INSERT...SELECT + DELETE
CREATE OR REPLACE PROCEDURE sp_archive_stale_policies(
    P_CUTOFF_YEARS  INT         DEFAULT 3,
    P_BATCH_SIZE    INT         DEFAULT 1000,
    P_DRY_RUN       SMALLINT    DEFAULT 1               -- [CONVERT] TINYINT → SMALLINT
)
RETURNS INT
LANGUAGE SQL
AS
$$
DECLARE
    cutoff_date     DATE;
    total_archived  INT := 0;
BEGIN
    -- [CONVERT] DATEADD(year, -@cutoff_years, GETDATE()) → DATEADD('YEAR', -P_CUTOFF_YEARS, CURRENT_DATE())
    cutoff_date := DATEADD('YEAR', -:P_CUTOFF_YEARS, CURRENT_DATE());

    SYSTEM$LOG('INFO', 'Archive run: cutoff=' || TO_CHAR(:cutoff_date, 'YYYYMMDD')
              || ' batch_size=' || TO_CHAR(:P_BATCH_SIZE)
              || ' dry_run=' || TO_CHAR(:P_DRY_RUN));

    IF (:P_DRY_RUN = 1) THEN
        -- Dry run: just count and log what would be archived
        SELECT COUNT(*) INTO :total_archived
        FROM fact_policy fp
        INNER JOIN dim_date dd ON dd.date_key = fp.expiry_date_key
        WHERE fp.status_code IN ('CN', 'EX')
          AND dd.calendar_date < :cutoff_date;

        SYSTEM$LOG('INFO', 'DRY RUN: Would archive ' || TO_CHAR(:total_archived) || ' policies');
    ELSE
        -- Set-based archive: use a temp table to identify the batch, then INSERT + DELETE atomically
        -- This replaces the per-row CURSOR + GOTO + BEGIN TRAN pattern

        CREATE OR REPLACE TEMPORARY TABLE _archive_batch AS
        SELECT fp.policy_id
        FROM fact_policy fp
        INNER JOIN dim_date dd ON dd.date_key = fp.expiry_date_key
        WHERE fp.status_code IN ('CN', 'EX')
          AND dd.calendar_date < :cutoff_date
        ORDER BY dd.calendar_date ASC
        LIMIT :P_BATCH_SIZE;

        -- Archive to history table
        -- [CONVERT] SELECT *, GETDATE() AS archived_ts → SELECT *, CURRENT_TIMESTAMP() AS archived_ts
        INSERT INTO fact_policy_archive
        SELECT fp.*, CURRENT_TIMESTAMP() AS archived_ts
        FROM fact_policy fp
        WHERE fp.policy_id IN (SELECT policy_id FROM _archive_batch);

        -- Delete archived rows from fact table
        DELETE FROM fact_policy
        WHERE policy_id IN (SELECT policy_id FROM _archive_batch);

        SELECT COUNT(*) INTO :total_archived FROM _archive_batch;

        DROP TABLE IF EXISTS _archive_batch;

        SYSTEM$LOG('INFO', 'Archive complete. Total: ' || TO_CHAR(:total_archived) || ' policies');
    END IF;

    RETURN :total_archived;

EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('ERROR', 'Archive failed: ' || SQLERRM);
        RAISE;
END;
$$


-- #############################################################################
-- OBJECT 11 of 12: sp_recalculate_kpis (ETL_Script_06.sql)
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_06.sql lines 251-298
-- Object : sp_recalculate_kpis (STORED PROCEDURE — dynamic SQL)
-- Series : enhanced
-- Layer  : procedure
-- Score  : 6 (Medium, MANUAL)
-- Changes: EXEC(@dyn_delete) → EXECUTE IMMEDIATE :dyn_delete           [CONVERT]
--          TINYINT → SMALLINT                                          [CONVERT]
--          CONVERT(VARCHAR, @val) → TO_CHAR(:val)                      [CONVERT]
--          @@ERROR → EXCEPTION                                         [CONVERT]
--          RAISERROR → RAISE                                           [CONVERT]
--          ISNULL → COALESCE                                           [CONVERT]
--          PRINT → SYSTEM$LOG                                          [CONVERT]
--          RIGHT('0' + CONVERT(VARCHAR, @m), 2) → LPAD(TO_CHAR(:m), 2, '0') [CONVERT]
--          GO → removed                                                [REMOVE]
-- =============================================================================

-- [CONVERT] EXEC(@dyn_delete) → EXECUTE IMMEDIATE :dyn_delete
CREATE OR REPLACE PROCEDURE sp_recalculate_kpis(
    P_YEAR_NUMBER   SMALLINT,
    P_MONTH_NUMBER  SMALLINT,                           -- [CONVERT] TINYINT → SMALLINT
    P_LOB_CODE      VARCHAR(10) DEFAULT NULL             -- NULL = all LOBs
)
RETURNS INT
LANGUAGE SQL
AS
$$
DECLARE
    dyn_delete  VARCHAR(2000);
    where_lob   VARCHAR(100);
BEGIN
    IF (:P_LOB_CODE IS NOT NULL) THEN
        where_lob := ' AND lob_code = ''' || :P_LOB_CODE || '''';
    ELSE
        where_lob := '';
    END IF;

    -- [TODO] SQL injection risk: P_LOB_CODE is concatenated into dynamic SQL.
    -- In production, use parameterized queries or validate P_LOB_CODE against
    -- an allowlist of known LOB codes before concatenation.

    -- Dynamic DELETE: remove existing aggregates for this period
    -- [CONVERT] EXEC(@dyn_delete) → EXECUTE IMMEDIATE :dyn_delete
    dyn_delete :=
        'DELETE FROM agg_policy_monthly '
        || 'WHERE year_number = ' || TO_CHAR(:P_YEAR_NUMBER)
        || ' AND month_number = ' || TO_CHAR(:P_MONTH_NUMBER)
        || :where_lob;

    SYSTEM$LOG('INFO', 'Executing: ' || :dyn_delete);
    EXECUTE IMMEDIATE :dyn_delete;

    -- [CONVERT] RIGHT('0' + CONVERT(VARCHAR, @month_number), 2) → LPAD(TO_CHAR(:P_MONTH_NUMBER), 2, '0')
    -- [CONVERT] ISNULL(' LOB=' + @lob_code, ' (all LOBs)') → COALESCE(' LOB=' || :P_LOB_CODE, ' (all LOBs)')
    SYSTEM$LOG('INFO', 'Recalculation complete for ' || TO_CHAR(:P_YEAR_NUMBER)
              || '-' || LPAD(TO_CHAR(:P_MONTH_NUMBER), 2, '0')
              || COALESCE(' LOB=' || :P_LOB_CODE, ' (all LOBs)'));

    RETURN 0;

EXCEPTION
    -- [CONVERT] IF @@ERROR <> 0 BEGIN RAISERROR ... RETURN -1 END → EXCEPTION block
    WHEN OTHER THEN
        LET err_msg VARCHAR := 'sp_recalculate_kpis: DELETE failed for '
            || TO_CHAR(:P_YEAR_NUMBER) || '/' || TO_CHAR(:P_MONTH_NUMBER)
            || '. Error: ' || SQLERRM;
        RAISE err_msg;
END;
$$


-- #############################################################################
-- OBJECT 12 of 12: ETL_Script_07.sql utility objects
-- Rules, Defaults, IQ Indexes, SET TEMPORARY OPTION
-- #############################################################################

-- =============================================================================
-- Source : ETL_Script_07.sql lines 1-132
-- Objects: 4 rules, 3 defaults, 7 sp_bindrule/sp_binddefault calls,
--          6 IQ indexes, 5 SET TEMPORARY OPTION statements
-- Series : enhanced
-- Layer  : utility / DDL
-- Score  : varies (Simple-Medium)
-- Changes: CREATE RULE → [REMOVE] with CHECK constraint equivalent     [REMOVE]
--          CREATE DEFAULT → [REMOVE] with DEFAULT equivalent           [REMOVE]
--          sp_bindrule → [REMOVE]                                      [REMOVE]
--          sp_binddefault → [REMOVE]                                   [REMOVE]
--          IQ UNIQUE(n) indexes → [REMOVE] with CLUSTER BY suggestion [REMOVE]
--          SET TEMPORARY OPTION → [REMOVE]                             [REMOVE]
--          GO → removed                                                [REMOVE]
-- =============================================================================


-- =============================================================================
-- SECTION 1: Rules → CHECK constraints
-- [REMOVE] CREATE RULE — Sybase rule objects have no Snowflake equivalent.
-- Equivalent CHECK constraints are shown below as informational comments (Snowflake does not enforce CHECK).
-- =============================================================================

-- [REMOVE] CREATE RULE rule_pct_range AS @value >= 0.0000 AND @value <= 1.0000
-- Equivalent: CHECK constraint on columns that store percentage as decimal 0-1
-- Example: ALTER TABLE <table> ADD CONSTRAINT chk_<col>_pct_range CHECK (<col> >= 0.0000 AND <col> <= 1.0000);

-- [REMOVE] CREATE RULE rule_gender_code AS @value IN ('M', 'F', 'X', 'U')
-- Equivalent: CHECK constraint on gender_code columns
-- Example: ALTER TABLE stg_customers ADD CONSTRAINT chk_gender_code CHECK (gender_code IN ('M', 'F', 'X', 'U'));
-- [TODO] Informational constraint (Snowflake does not enforce CHECK):
-- ALTER TABLE stg_customers ADD CONSTRAINT chk_stg_customers_gender_code
--     CHECK (gender_code IS NULL OR gender_code IN ('M', 'F', 'X', 'U'));

-- [REMOVE] CREATE RULE rule_claim_type AS @value IN ('BI','PD','COMP','COLL','MED','UM','UIM','PIP','GL','PROP','LIAB','OTHER')
-- Equivalent: CHECK constraint on claim_type_code columns
-- [TODO] Informational constraint (Snowflake does not enforce CHECK):
-- ALTER TABLE stg_claims ADD CONSTRAINT chk_stg_claims_claim_type_code
--     CHECK (claim_type_code IS NULL OR claim_type_code IN ('BI','PD','COMP','COLL','MED','UM','UIM','PIP','GL','PROP','LIAB','OTHER'));

-- [REMOVE] CREATE RULE rule_treaty_type AS @value IN ('QS','XL','FAC','STOP_LOSS','CAT_XL')
-- Equivalent: CHECK constraint on treaty_type columns
-- [TODO] Informational constraint (Snowflake does not enforce CHECK):
-- ALTER TABLE stg_reinsurance ADD CONSTRAINT chk_stg_reinsurance_treaty_type
--     CHECK (treaty_type IS NULL OR treaty_type IN ('QS','XL','FAC','STOP_LOSS','CAT_XL'));


-- =============================================================================
-- SECTION 2: Defaults → DEFAULT constraints
-- [REMOVE] CREATE DEFAULT — Sybase default objects have no Snowflake equivalent.
-- Equivalent DEFAULT values are shown as ALTER TABLE statements.
-- =============================================================================

-- [REMOVE] CREATE DEFAULT def_unknown_str AS 'UNKNOWN'
-- Equivalent: ALTER TABLE <table> ALTER COLUMN <col> SET DEFAULT 'UNKNOWN';

-- [REMOVE] CREATE DEFAULT def_zero_int AS 0
-- Equivalent: ALTER TABLE <table> ALTER COLUMN <col> SET DEFAULT 0;

-- [REMOVE] CREATE DEFAULT def_far_future AS '9999-12-31'
-- Equivalent: ALTER TABLE <table> ALTER COLUMN <col> SET DEFAULT '9999-12-31';


-- =============================================================================
-- SECTION 2b: sp_bindrule / sp_binddefault → inlined as constraints
-- [REMOVE] All sp_bindrule and sp_binddefault calls
-- =============================================================================

-- [REMOVE] EXEC sp_bindrule 'rule_pct_range', 'stg_reinsurance.reinsurer_share_pct'
-- [TODO] Informational constraint (Snowflake does not enforce CHECK):
-- ALTER TABLE stg_reinsurance ADD CONSTRAINT chk_stg_reinsurance_pct_range
--     CHECK (reinsurer_share_pct IS NULL OR (reinsurer_share_pct >= 0.0000 AND reinsurer_share_pct <= 1.0000));

-- [REMOVE] EXEC sp_bindrule 'rule_treaty_type', 'stg_reinsurance.treaty_type'
-- Already handled above (chk_stg_reinsurance_treaty_type)

-- [REMOVE] EXEC sp_bindrule 'rule_yn_flag', 'stg_reinsurance.reinstatement_flag'
-- [TODO] Informational constraint (Snowflake does not enforce CHECK):
-- ALTER TABLE stg_reinsurance ADD CONSTRAINT chk_stg_reinsurance_reinstatement_flag
--     CHECK (reinstatement_flag IS NULL OR reinstatement_flag IN ('Y', 'N'));

-- [REMOVE] EXEC sp_binddefault 'def_zero_int', 'stg_reinsurance.layer_number'
-- Inlined as:
ALTER TABLE stg_reinsurance ALTER COLUMN layer_number SET DEFAULT 0;

-- [REMOVE] EXEC sp_bindrule 'rule_gender_code', 'stg_customers.gender_code'
-- Already handled above (chk_stg_customers_gender_code)

-- [REMOVE] EXEC sp_bindrule 'rule_claim_type', 'stg_claims.claim_type_code'
-- Already handled above (chk_stg_claims_claim_type_code)


-- =============================================================================
-- SECTION 3: IQ-Specific Indexes → [REMOVE]
-- Snowflake uses micro-partitioning; no traditional indexes.
-- Suggest CLUSTER BY for frequently filtered columns.
-- =============================================================================

-- [REMOVE] CREATE INDEX idx_fact_policy_status ON fact_policy (status_code) IQ UNIQUE (8)
-- Suggested: ALTER TABLE fact_policy CLUSTER BY (status_code);
-- Note: Only add CLUSTER BY if queries frequently filter/sort by this column.

-- [REMOVE] CREATE INDEX idx_fact_policy_product ON fact_policy (product_key) IQ UNIQUE (500)
-- Suggested: Consider including product_key in CLUSTER BY if frequently used in joins/filters.

-- [REMOVE] CREATE INDEX idx_fact_claims_type ON fact_claims (claim_type_code) IQ UNIQUE (12)
-- Suggested: ALTER TABLE fact_claims CLUSTER BY (claim_type_code);

-- [REMOVE] CREATE INDEX idx_fact_claims_status ON fact_claims (status_code) IQ UNIQUE (8)
-- Suggested: Consider CLUSTER BY (claim_type_code, status_code) for multi-column filtering.

-- [REMOVE] CREATE INDEX idx_agg_policy_year_month ON agg_policy_monthly (year_number, month_number) IQ UNIQUE (240)
-- Suggested: ALTER TABLE agg_policy_monthly CLUSTER BY (year_number, month_number);

-- [REMOVE] CREATE INDEX idx_dim_customer_segment ON dim_customer (customer_segment) IQ UNIQUE (10)
-- Suggested: ALTER TABLE dim_customer CLUSTER BY (customer_segment);

-- [TODO] Evaluate query patterns before applying CLUSTER BY. Snowflake's automatic
-- micro-partition pruning may suffice without explicit clustering.
-- Run: SELECT SYSTEM$CLUSTERING_INFORMATION('fact_policy', '(status_code)');
-- to assess whether clustering would improve pruning.


-- =============================================================================
-- SECTION 4: SET TEMPORARY OPTION → [REMOVE]
-- Sybase IQ session-level query tuning. Snowflake handles optimization
-- automatically via its query optimizer. No equivalent needed.
-- =============================================================================

-- [REMOVE] SET TEMPORARY OPTION Query_Plan_As_Html = 'OFF'
-- No Snowflake equivalent. Query plans are accessed via EXPLAIN or query profile UI.

-- [REMOVE] SET TEMPORARY OPTION Query_Rows_Returned_Limit = '0'
-- No Snowflake equivalent. Use LIMIT clause if row restriction is needed.

-- [REMOVE] SET TEMPORARY OPTION Query_Plan_After_Run = 'OFF'
-- No Snowflake equivalent. Use EXPLAIN plan or query profile as needed.

-- [REMOVE] SET TEMPORARY OPTION Forced_Predicate_Pushdown = 'ALL'
-- Snowflake handles predicate pushdown automatically.

-- [REMOVE] SET TEMPORARY OPTION Hash_Pinnable_Cache = '75'
-- Snowflake manages caching internally (result cache, warehouse cache).


-- =============================================================================
-- END OF COMPLEX-TIER CONVERSION
-- Total objects converted: 12
-- Quarantine candidates: 4 (etl_audit_log enhanced, agg_policy_monthly,
--                           trg_kpi_view_readonly, sp_archive_stale_policies)
-- Manual review items: sp_recalculate_kpis (SQL injection risk in dynamic SQL)
-- =============================================================================
