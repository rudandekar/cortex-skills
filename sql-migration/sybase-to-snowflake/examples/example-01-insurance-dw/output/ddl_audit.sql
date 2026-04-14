-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql4.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L4_audit (DDL)
-- [META] Objects:       etl_audit_log
-- =============================================================================

-- ============================================================
-- ETL Audit Log Table
-- [CONVERT] IF NOT EXISTS(...sysobjects...) BEGIN CREATE TABLE END
--           → CREATE TABLE IF NOT EXISTS (Snowflake native)
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id            INT AUTOINCREMENT   NOT NULL PRIMARY KEY,
    batch_id            INT                 NOT NULL,
    run_date            DATE                NOT NULL,
    run_ts              TIMESTAMP_NTZ       NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    script_name         VARCHAR(100)        NOT NULL,
    table_name          VARCHAR(100)        NOT NULL,
    check_type          VARCHAR(50)         NOT NULL,
    rows_expected       INT                 NULL,
    rows_actual         INT                 NULL,
    check_result        VARCHAR(10)         NOT NULL,  -- PASS / FAIL / WARN
    threshold_pct       DECIMAL(6,2)        NULL,
    variance_pct        DECIMAL(8,4)        NULL,
    error_message       VARCHAR(500)        NULL,
    created_ts          TIMESTAMP_NTZ       NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
