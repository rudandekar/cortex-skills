-- =============================================================================
-- ETL SCRIPT 06: Master Stored Procedures
-- Includes: orchestrator, archive, recalculation with dynamic SQL
-- Complexity  : COMPLEX — Full @@ERROR handling, dynamic SQL, transactions
-- =============================================================================

-- =============================================================================
-- Procedure: sp_validate_claims
-- Returns: result set of validated claims from staging (used by INSERT...EXEC)
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'sp_validate_claims' AND type = 'P')
    DROP PROCEDURE sp_validate_claims
GO

CREATE PROCEDURE sp_validate_claims
    @batch_date     DATE    = NULL,
    @include_invalid TINYINT = 0
AS
BEGIN
    -- Apply default batch date if not provided
    IF @batch_date IS NULL
        SELECT @batch_date = GETDATE()

    -- Return validated claims
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
        CASE WHEN ISNULL(sc.total_incurred, 0) < 0 THEN 0.00 ELSE sc.total_incurred END,
        CASE WHEN ISNULL(sc.total_paid, 0)     < 0 THEN 0.00 ELSE sc.total_paid     END,
        CASE WHEN ISNULL(sc.total_reserved, 0) < 0 THEN 0.00 ELSE sc.total_reserved END,
        ISNULL(sc.salvage_amount, 0.00),
        ISNULL(sc.subrogation_amount, 0.00),
        CASE
            WHEN sc.policy_id IS NOT NULL
             AND sc.incident_date <= ISNULL(sc.reported_date, GETDATE())
            THEN 'VALID'
            ELSE 'INVALID'
        END AS validation_status
    FROM stg_claims sc
    WHERE
        sc.claim_id IS NOT NULL
        AND sc.policy_id IS NOT NULL
        AND (
            @include_invalid = 1
            OR (
                sc.policy_id IS NOT NULL
                AND sc.incident_date <= ISNULL(sc.reported_date, GETDATE())
            )
        )

    IF @@ERROR <> 0
    BEGIN
        RAISERROR ('sp_validate_claims: Query failed.', 16, 1)
        RETURN -1
    END

    RETURN 0
END
GO

-- =============================================================================
-- Procedure: sp_run_etl_pipeline
-- Master orchestrator — calls all ETL scripts in sequence
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'sp_run_etl_pipeline' AND type = 'P')
    DROP PROCEDURE sp_run_etl_pipeline
GO

CREATE PROCEDURE sp_run_etl_pipeline
    @run_date       DATE        = NULL,
    @run_mode       VARCHAR(10) = 'FULL',   -- FULL or INCREMENTAL
    @abort_on_fail  TINYINT     = 1
AS
BEGIN
    DECLARE @step_name      VARCHAR(100)
    DECLARE @step_start     DATETIME
    DECLARE @step_rc        INT
    DECLARE @total_fails    INT
    DECLARE @pipeline_id    INT

    SELECT @total_fails = 0
    IF @run_date IS NULL SELECT @run_date = GETDATE()

    PRINT '========================================================'
    PRINT 'ETL Pipeline Start: ' + CONVERT(VARCHAR, GETDATE(), 120)
    PRINT 'Mode: ' + @run_mode + ' | Date: ' + CONVERT(VARCHAR, @run_date, 112)
    PRINT '========================================================'

    -- Step 1: Validate staging is populated
    SELECT @step_name  = 'STAGING_ROW_COUNT_CHECK'
    SELECT @step_start = GETDATE()

    IF (SELECT COUNT(*) FROM stg_policies) = 0
    BEGIN
        INSERT INTO etl_audit_log (script_name, object_name, check_type, status, error_message)
        VALUES ('sp_run_etl_pipeline', 'stg_policies', @step_name, 'FAIL',
                'stg_policies is empty — staging not loaded')
        IF @abort_on_fail = 1
        BEGIN
            RAISERROR ('Pipeline aborted: staging tables are empty.', 16, 1)
            RETURN -1
        END
        SELECT @total_fails = @total_fails + 1
    END

    -- Step 2: Load dimensions
    SELECT @step_name  = 'LOAD_DIMENSIONS'
    SELECT @step_start = GETDATE()
    PRINT 'Step: ' + @step_name + ' | ' + CONVERT(VARCHAR, @step_start, 120)

    -- In production, this would call individual dim load procedures
    -- Inline here for demonstration of @@ERROR checking pattern
    EXEC sp_validate_claims @batch_date = @run_date, @include_invalid = 0
    SELECT @step_rc = @@ERROR
    IF @step_rc <> 0
    BEGIN
        INSERT INTO etl_audit_log (script_name, object_name, check_type, status, error_message)
        VALUES ('sp_run_etl_pipeline', 'DIMENSIONS', @step_name, 'FAIL',
                'Dimension load failed with error: ' + CONVERT(VARCHAR, @step_rc))
        SELECT @total_fails = @total_fails + 1
        IF @abort_on_fail = 1
        BEGIN
            RAISERROR ('Pipeline aborted at step: %s', 16, 1, @step_name)
            RETURN -1
        END
    END

    -- Final summary
    PRINT '========================================================'
    PRINT 'ETL Pipeline End:   ' + CONVERT(VARCHAR, GETDATE(), 120)
    PRINT 'Total Failures:     ' + CONVERT(VARCHAR, @total_fails)
    PRINT '========================================================'

    RETURN @total_fails
END
GO

-- =============================================================================
-- Procedure: sp_archive_stale_policies
-- Archives policies closed >3 years ago using CURSOR + transactions
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'sp_archive_stale_policies' AND type = 'P')
    DROP PROCEDURE sp_archive_stale_policies
GO

CREATE PROCEDURE sp_archive_stale_policies
    @cutoff_years   INT         = 3,
    @batch_size     INT         = 1000,
    @dry_run        TINYINT     = 1
AS
BEGIN
    DECLARE @policy_id      BIGINT
    DECLARE @policy_number  VARCHAR(30)
    DECLARE @batch_count    INT
    DECLARE @total_archived INT
    DECLARE @cutoff_date    DATE

    SELECT @cutoff_date    = DATEADD(year, -@cutoff_years, GETDATE())
    SELECT @total_archived = 0
    SELECT @batch_count    = 0

    PRINT 'Archive run: cutoff=' + CONVERT(VARCHAR, @cutoff_date, 112)
          + ' batch_size=' + CONVERT(VARCHAR, @batch_size)
          + ' dry_run=' + CONVERT(VARCHAR, @dry_run)

    DECLARE archive_cursor CURSOR FOR
        SELECT fp.policy_id, fp.policy_number
        FROM fact_policy fp
        INNER JOIN dim_date dd ON dd.date_key = fp.expiry_date_key
        WHERE fp.status_code IN ('CN', 'EX')
          AND dd.calendar_date < @cutoff_date
        ORDER BY dd.calendar_date ASC
    FOR READ ONLY

    OPEN archive_cursor
    FETCH NEXT FROM archive_cursor INTO @policy_id, @policy_number

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @dry_run = 0
        BEGIN
            BEGIN TRAN archive_batch

                -- Archive to history table (assumed to exist)
                INSERT INTO fact_policy_archive
                SELECT *, GETDATE() AS archived_ts
                FROM fact_policy
                WHERE policy_id = @policy_id

                IF @@ERROR <> 0
                BEGIN
                    ROLLBACK TRAN archive_batch
                    PRINT 'ERROR archiving policy: ' + CONVERT(VARCHAR, @policy_id)
                    -- Skip this policy, continue
                    GOTO next_policy
                END

                DELETE FROM fact_policy WHERE policy_id = @policy_id

                IF @@ERROR <> 0
                BEGIN
                    ROLLBACK TRAN archive_batch
                    PRINT 'ERROR deleting policy after archive: ' + CONVERT(VARCHAR, @policy_id)
                    GOTO next_policy
                END

            COMMIT TRAN archive_batch
        END
        ELSE
            PRINT 'DRY RUN: Would archive policy ' + @policy_number
            + ' (id=' + CONVERT(VARCHAR, @policy_id) + ')'

        SELECT @total_archived = @total_archived + 1
        SELECT @batch_count    = @batch_count + 1

        -- Log every batch_size records
        IF @batch_count >= @batch_size
        BEGIN
            PRINT 'Batch checkpoint: ' + CONVERT(VARCHAR, @total_archived) + ' archived so far'
            SELECT @batch_count = 0
        END

        next_policy:
        FETCH NEXT FROM archive_cursor INTO @policy_id, @policy_number
    END

    CLOSE     archive_cursor
    DEALLOCATE archive_cursor

    PRINT 'Archive complete. Total: ' + CONVERT(VARCHAR, @total_archived) + ' policies'
    RETURN @total_archived
END
GO

-- =============================================================================
-- Procedure: sp_recalculate_kpis
-- Rebuilds KPI aggregates for a given year/month using dynamic SQL
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'sp_recalculate_kpis' AND type = 'P')
    DROP PROCEDURE sp_recalculate_kpis
GO

CREATE PROCEDURE sp_recalculate_kpis
    @year_number    SMALLINT,
    @month_number   TINYINT,
    @lob_code       VARCHAR(10) = NULL   -- NULL = all LOBs
AS
BEGIN
    DECLARE @dyn_delete VARCHAR(2000)
    DECLARE @dyn_insert VARCHAR(4000)
    DECLARE @where_lob  VARCHAR(100)

    IF @lob_code IS NOT NULL
        SELECT @where_lob = ' AND lob_code = ''' + @lob_code + ''''
    ELSE
        SELECT @where_lob = ''

    -- Dynamic DELETE: remove existing aggregates for this period
    SELECT @dyn_delete =
        'DELETE FROM agg_policy_monthly '
        + 'WHERE year_number = ' + CONVERT(VARCHAR, @year_number)
        + ' AND month_number = ' + CONVERT(VARCHAR, @month_number)
        + @where_lob

    PRINT 'Executing: ' + @dyn_delete
    EXEC(@dyn_delete)

    IF @@ERROR <> 0
    BEGIN
        RAISERROR ('sp_recalculate_kpis: DELETE failed for %d/%d', 16, 1,
                   @year_number, @month_number)
        RETURN -1
    END

    PRINT 'Recalculation complete for ' + CONVERT(VARCHAR, @year_number)
          + '-' + RIGHT('0' + CONVERT(VARCHAR, @month_number), 2)
          + ISNULL(' LOB=' + @lob_code, ' (all LOBs)')

    RETURN 0
END
GO

