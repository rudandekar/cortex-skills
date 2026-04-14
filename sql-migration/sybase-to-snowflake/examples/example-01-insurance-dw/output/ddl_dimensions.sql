-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql2.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L2_dimension (DDL)
-- [META] Objects:       dim_date, dim_product, dim_agent, dim_customer
-- =============================================================================

-- ============================================================
-- SECTION 1: DIM_DATE (static reference, load once per year)
-- [CONVERT] IF EXISTS/DROP/GO → CREATE OR REPLACE
-- [CONVERT] TINYINT → SMALLINT (Snowflake minimum integer)
-- ============================================================

CREATE OR REPLACE TABLE dim_date (
    date_key            INT             NOT NULL PRIMARY KEY,  -- YYYYMMDD
    calendar_date       DATE            NOT NULL,
    day_of_week         SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    day_name            VARCHAR(10)     NOT NULL,
    day_of_month        SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    day_of_year         SMALLINT        NOT NULL,
    week_of_year        SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    month_number        SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    month_name          VARCHAR(10)     NOT NULL,
    month_short         CHAR(3)         NOT NULL,
    quarter_number      SMALLINT        NOT NULL,              -- [CONVERT] TINYINT → SMALLINT
    quarter_name        CHAR(2)         NOT NULL,
    year_number         SMALLINT        NOT NULL,
    fiscal_month        SMALLINT        NULL,                  -- [CONVERT] TINYINT → SMALLINT
    fiscal_quarter      SMALLINT        NULL,                  -- [CONVERT] TINYINT → SMALLINT
    fiscal_year         SMALLINT        NULL,
    is_weekday          CHAR(1)         NOT NULL DEFAULT 'Y',
    is_holiday          CHAR(1)         NOT NULL DEFAULT 'N',
    holiday_name        VARCHAR(50)     NULL,
    is_month_end        CHAR(1)         NOT NULL DEFAULT 'N',
    is_quarter_end      CHAR(1)         NOT NULL DEFAULT 'N',
    is_year_end         CHAR(1)         NOT NULL DEFAULT 'N'
);

-- ============================================================
-- SECTION 2: DIM_PRODUCT (SCD Type 1 - overwrite)
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- ============================================================

CREATE OR REPLACE TABLE dim_product (
    product_key         INT AUTOINCREMENT NOT NULL PRIMARY KEY,  -- [CONVERT] IDENTITY → AUTOINCREMENT
    product_code        VARCHAR(20)       NOT NULL,
    product_name        VARCHAR(150)      NOT NULL,
    product_line        VARCHAR(50)       NOT NULL,
    lob_code            VARCHAR(20)       NULL,
    lob_description     VARCHAR(100)      NULL,
    sub_lob_code        VARCHAR(20)       NULL,
    coverage_type       VARCHAR(50)       NULL,
    rating_plan         VARCHAR(30)       NULL,
    min_premium         DECIMAL(18,2)     NULL,
    max_coverage        DECIMAL(18,2)     NULL,
    is_active           CHAR(1)           NOT NULL DEFAULT 'Y',
    effective_date      DATE              NULL,
    discontinue_date    DATE              NULL,
    dw_insert_ts        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- SECTION 3: DIM_AGENT (SCD Type 2 - preserve history)
-- [CONVERT] SMALLINT scd_version preserved
-- ============================================================

CREATE OR REPLACE TABLE dim_agent (
    agent_key           INT AUTOINCREMENT NOT NULL PRIMARY KEY,  -- [CONVERT] IDENTITY → AUTOINCREMENT
    agent_id            INT               NOT NULL,              -- natural key
    agent_number        VARCHAR(20)       NOT NULL,
    agent_name          VARCHAR(200)      NOT NULL,
    agency_id           INT               NULL,
    agency_name         VARCHAR(200)      NULL,
    license_state       CHAR(2)           NULL,
    license_number      VARCHAR(30)       NULL,
    license_expiry      DATE              NULL,
    region_code         VARCHAR(10)       NULL,
    region_description  VARCHAR(100)      NULL,
    channel_code        VARCHAR(20)       NULL,
    channel_description VARCHAR(100)      NULL,
    is_active           CHAR(1)           NOT NULL DEFAULT 'Y',
    hire_date           DATE              NULL,
    -- SCD2 tracking columns
    scd_start_date      DATE              NOT NULL,
    scd_end_date        DATE              NULL,       -- NULL = current record
    scd_current_flag    CHAR(1)           NOT NULL DEFAULT 'Y',
    scd_version         SMALLINT          NOT NULL DEFAULT 1,
    dw_insert_ts        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- SECTION 4: DIM_CUSTOMER (SCD Type 2 - preserve history)
-- ============================================================

CREATE OR REPLACE TABLE dim_customer (
    customer_key        BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY,  -- [CONVERT] IDENTITY → AUTOINCREMENT
    customer_id         BIGINT               NOT NULL,              -- natural key
    full_name           VARCHAR(200)         NULL,
    first_name          VARCHAR(100)         NULL,
    last_name           VARCHAR(100)         NULL,
    date_of_birth       DATE                 NULL,
    age_band            VARCHAR(20)          NULL,
    gender_code         CHAR(1)              NULL,
    gender_description  VARCHAR(10)          NULL,
    marital_status      CHAR(1)              NULL,
    marital_description VARCHAR(20)          NULL,
    city                VARCHAR(100)         NULL,
    state_code          CHAR(2)              NULL,
    state_name          VARCHAR(50)          NULL,
    zip_code            VARCHAR(10)          NULL,
    region              VARCHAR(30)          NULL,
    credit_score        SMALLINT             NULL,
    credit_band         VARCHAR(20)          NULL,
    customer_segment    VARCHAR(30)          NULL,
    acquisition_source  VARCHAR(50)          NULL,
    -- SCD2 tracking columns
    scd_start_date      DATE                 NOT NULL,
    scd_end_date        DATE                 NULL,
    scd_current_flag    CHAR(1)              NOT NULL DEFAULT 'Y',
    scd_version         SMALLINT             NOT NULL DEFAULT 1,
    dw_insert_ts        TIMESTAMP_NTZ        NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ        NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
