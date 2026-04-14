-- =============================================================================
-- COMPLEX TIER: Dimensions & Fact Tables — Sybase → Snowflake Conversion
-- =============================================================================
-- Generated  : 2026-04-02
-- Provenance : sql2.sql, sql3.sql, ETL_Script_02.sql, ETL_Script_03.sql
-- Tier       : COMPLEX (Score 5–17)
-- Converter  : Manual rule-based migration per sybase-to-snowflake type-map
-- =============================================================================
--
-- Conversion Summary:
--   Objects converted: 14
--   Patterns applied:
--     - WHILE loop         → GENERATOR() + CTE           (dim_date x2)
--     - SCD1 UPDATE+INSERT → MERGE INTO                  (dim_product x2, dim_territory)
--     - SCD2 expire+insert → UPDATE then INSERT           (dim_agent x2, dim_customer x2)
--     - CURSOR + GOTO      → set-based CTE + EXCEPTION   (dim_agent enhanced)
--     - #temp tables        → TEMPORARY TABLE              (dim_customer enhanced, fact_policy enhanced, fact_claims enhanced)
--     - BEGIN TRAN/@@ERROR  → Snowflake Scripting EXCEPTION (fact_policy enhanced, fact_claims enhanced, dim_customer enhanced)
--     - INSERT...EXEC       → direct SELECT + CTE          (fact_claims enhanced)
--     - COMPUTE BY          → [REMOVE] + GROUP BY comment  (dim_territory)
--     - CONVERT(INT,CONVERT(CHAR(8),d,112)) → TO_NUMBER(TO_CHAR(d,'YYYYMMDD'))
--     - GETDATE()           → CURRENT_TIMESTAMP()
--     - ISNULL()            → COALESCE()
--     - DATETIME            → TIMESTAMP_NTZ
--     - TINYINT             → SMALLINT
--     - MONEY               → NUMBER(19,4)
--     - DECIMAL(p,s)        → NUMBER(p,s)
--     - IDENTITY            → AUTOINCREMENT
--     - IN DBASPACE         → [REMOVE]
--     - GO                  → [REMOVE]
--     - PRINT               → [REMOVE] (or SYSTEM$LOG where noted)
-- =============================================================================


-- #############################################################################
-- OBJECT 1 OF 14: dim_date (basic) — Source: sql2.sql, Section 1
-- Score: 8 | Complexity: Complex
-- Pattern: WHILE loop → GENERATOR() + CTE
-- #############################################################################
-- Provenance: sql2.sql lines 31–217
-- Sybase: IF EXISTS/DROP + CREATE + WHILE loop inserting one row per day
--         from 2000-01-01 to 2030-12-31, then UPDATE for quarter_end
-- [CONVERT] IF EXISTS/DROP TABLE → CREATE OR REPLACE TABLE
-- [CONVERT] WHILE loop → GENERATOR(ROWCOUNT => 11323) + CTE date_spine
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] DATEPART(WEEKDAY, d) → MOD(DAYOFWEEKISO(d), 7) + 1  (1=Sun..7=Sat compatibility)
-- [CONVERT] DATENAME(WEEKDAY, d) → DAYNAME(d)
-- [CONVERT] DATENAME(MONTH, d) → MONTHNAME(d)
-- [CONVERT] DATEPART(x, d) → equivalent Snowflake functions
-- [CONVERT] TINYINT → SMALLINT
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE dim_date (
    date_key            INT             NOT NULL PRIMARY KEY,  -- YYYYMMDD
    calendar_date       DATE            NOT NULL,
    day_of_week         SMALLINT        NOT NULL,   -- 1=Sun, 7=Sat (Sybase-compatible)
    day_name            VARCHAR(10)     NOT NULL,
    day_of_month        SMALLINT        NOT NULL,
    day_of_year         SMALLINT        NOT NULL,
    week_of_year        SMALLINT        NOT NULL,
    month_number        SMALLINT        NOT NULL,
    month_name          VARCHAR(10)     NOT NULL,
    month_short         CHAR(3)         NOT NULL,
    quarter_number      SMALLINT        NOT NULL,
    quarter_name        CHAR(2)         NOT NULL,
    year_number         SMALLINT        NOT NULL,
    fiscal_month        SMALLINT        NULL,       -- fiscal year April start
    fiscal_quarter      SMALLINT        NULL,
    fiscal_year         SMALLINT        NULL,
    is_weekday          CHAR(1)         NOT NULL DEFAULT 'Y',
    is_holiday          CHAR(1)         NOT NULL DEFAULT 'N',
    holiday_name        VARCHAR(50)     NULL,
    is_month_end        CHAR(1)         NOT NULL DEFAULT 'N',
    is_quarter_end      CHAR(1)         NOT NULL DEFAULT 'N',
    is_year_end         CHAR(1)         NOT NULL DEFAULT 'N'
);

-- [CONVERT] WHILE loop → GENERATOR() + CTE (type-map §3: WHILE Loop / §5)
INSERT INTO dim_date
WITH date_spine AS (
    SELECT DATEADD('DAY', SEQ4(), '2000-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 11323))  -- 2000-01-01 to 2030-12-31 inclusive
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))           AS date_key,
    calendar_date,
    MOD(DAYOFWEEKISO(calendar_date), 7) + 1                 AS day_of_week,      -- 1=Sun..7=Sat
    DAYNAME(calendar_date)                                   AS day_name,
    DAY(calendar_date)                                       AS day_of_month,
    DAYOFYEAR(calendar_date)                                 AS day_of_year,
    WEEKOFYEAR(calendar_date)                                AS week_of_year,
    MONTH(calendar_date)                                     AS month_number,
    MONTHNAME(calendar_date)                                 AS month_name,
    LEFT(MONTHNAME(calendar_date), 3)                        AS month_short,
    QUARTER(calendar_date)                                   AS quarter_number,
    'Q' || CAST(QUARTER(calendar_date) AS CHAR(1))           AS quarter_name,
    YEAR(calendar_date)                                      AS year_number,
    -- Fiscal year offset by 3 months (April start)
    CASE WHEN MONTH(calendar_date) >= 4
         THEN MONTH(calendar_date) - 3
         ELSE MONTH(calendar_date) + 9
    END                                                      AS fiscal_month,
    CASE WHEN MONTH(calendar_date) BETWEEN 4 AND 6   THEN 1
         WHEN MONTH(calendar_date) BETWEEN 7 AND 9   THEN 2
         WHEN MONTH(calendar_date) BETWEEN 10 AND 12 THEN 3
         ELSE 4
    END                                                      AS fiscal_quarter,
    CASE WHEN MONTH(calendar_date) >= 4
         THEN YEAR(calendar_date)
         ELSE YEAR(calendar_date) - 1
    END                                                      AS fiscal_year,
    CASE WHEN MOD(DAYOFWEEKISO(calendar_date), 7) + 1 IN (1, 7)
         THEN 'N' ELSE 'Y'
    END                                                      AS is_weekday,
    'N'                                                      AS is_holiday,
    NULL                                                     AS holiday_name,
    CASE WHEN calendar_date = LAST_DAY(calendar_date)
         THEN 'Y' ELSE 'N'
    END                                                      AS is_month_end,
    -- Quarter end: last day of month AND month in (3,6,9,12)
    CASE WHEN calendar_date = LAST_DAY(calendar_date)
          AND MONTH(calendar_date) IN (3, 6, 9, 12)
         THEN 'Y' ELSE 'N'
    END                                                      AS is_quarter_end,
    CASE WHEN MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31
         THEN 'Y' ELSE 'N'
    END                                                      AS is_year_end
FROM date_spine;


-- #############################################################################
-- OBJECT 2 OF 14: dim_date (enhanced) — Source: ETL_Script_02.sql, Section 1
-- Score: 10 | Complexity: Complex
-- Pattern: WHILE loop → GENERATOR() + CTE (with season_code, CHAR(6) quarter_name)
-- #############################################################################
-- Provenance: ETL_Script_02.sql lines 9–127
-- Sybase: IF EXISTS/DROP + CREATE IN DBASPACE + WHILE loop 2000-01-01 to 2035-12-31
--         with season_code, CHAR(6) quarter_name like 'Q1-2024', is_last_day_month,
--         is_last_day_quarter, then UPDATE for quarter end.
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] WHILE loop → GENERATOR(ROWCOUNT => 13149) + CTE
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] CONVERT(INT, CONVERT(VARCHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] DATEPART(x, d) → Snowflake equivalents
-- [CONVERT] DATENAME(month/weekday) → MONTHNAME/DAYNAME
-- [CONVERT] STR(year,4) + month tricks for last-day → LAST_DAY()
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- =============================================================================

CREATE OR REPLACE TABLE dim_date_enhanced (
    date_key            INT             NOT NULL PRIMARY KEY,  -- YYYYMMDD integer
    calendar_date       DATE            NOT NULL,
    year_number         SMALLINT        NOT NULL,
    quarter_number      SMALLINT        NOT NULL,
    quarter_name        CHAR(7)         NOT NULL,   -- 'Q1-2024' format (7 chars for 4-digit year)
    month_number        SMALLINT        NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    month_abbrev        CHAR(3)         NOT NULL,
    week_of_year        SMALLINT        NOT NULL,
    day_of_week         SMALLINT        NOT NULL,   -- 1=Sun, 7=Sat (Sybase-compatible)
    day_name            VARCHAR(20)     NOT NULL,
    day_of_month        SMALLINT        NOT NULL,
    day_of_year         SMALLINT        NOT NULL,
    fiscal_year         SMALLINT        NOT NULL,
    fiscal_quarter      SMALLINT        NOT NULL,
    fiscal_month        SMALLINT        NOT NULL,
    is_weekend          CHAR(1)         NOT NULL,
    is_holiday          CHAR(1)         NOT NULL,
    is_last_day_month   CHAR(1)         NOT NULL,
    is_last_day_quarter CHAR(1)         NOT NULL,
    season_code         VARCHAR(10)     NOT NULL
);

-- [CONVERT] WHILE loop → GENERATOR() + CTE (type-map §3: WHILE Loop / §5)
-- Range: 2000-01-01 to 2035-12-31 = 13149 days
INSERT INTO dim_date_enhanced
WITH date_spine AS (
    SELECT DATEADD('DAY', SEQ4(), '2000-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 13149))  -- 2000-01-01 to 2035-12-31 inclusive
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))           AS date_key,
    calendar_date,
    YEAR(calendar_date)                                      AS year_number,
    QUARTER(calendar_date)                                   AS quarter_number,
    'Q' || CAST(QUARTER(calendar_date) AS CHAR(1))
        || '-' || CAST(YEAR(calendar_date) AS CHAR(4))       AS quarter_name,    -- e.g. 'Q1-2024'
    MONTH(calendar_date)                                     AS month_number,
    MONTHNAME(calendar_date)                                 AS month_name,
    LEFT(MONTHNAME(calendar_date), 3)                        AS month_abbrev,
    WEEKOFYEAR(calendar_date)                                AS week_of_year,
    MOD(DAYOFWEEKISO(calendar_date), 7) + 1                  AS day_of_week,     -- 1=Sun..7=Sat
    DAYNAME(calendar_date)                                   AS day_name,
    DAY(calendar_date)                                       AS day_of_month,
    DAYOFYEAR(calendar_date)                                 AS day_of_year,
    -- Fiscal year: April start
    CASE WHEN MONTH(calendar_date) >= 4
         THEN YEAR(calendar_date)
         ELSE YEAR(calendar_date) - 1
    END                                                      AS fiscal_year,
    -- Fiscal quarter from fiscal_month
    CASE
        WHEN MONTH(calendar_date) BETWEEN 4 AND 6   THEN 1
        WHEN MONTH(calendar_date) BETWEEN 7 AND 9   THEN 2
        WHEN MONTH(calendar_date) BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END                                                      AS fiscal_quarter,
    CASE WHEN MONTH(calendar_date) >= 4
         THEN MONTH(calendar_date) - 3
         ELSE MONTH(calendar_date) + 9
    END                                                      AS fiscal_month,
    CASE WHEN MOD(DAYOFWEEKISO(calendar_date), 7) + 1 IN (1, 7)
         THEN 'Y' ELSE 'N'
    END                                                      AS is_weekend,
    'N'                                                      AS is_holiday,   -- Populated separately by holiday proc
    CASE WHEN calendar_date = LAST_DAY(calendar_date)
         THEN 'Y' ELSE 'N'
    END                                                      AS is_last_day_month,
    CASE WHEN calendar_date = LAST_DAY(calendar_date)
          AND MONTH(calendar_date) IN (3, 6, 9, 12)
         THEN 'Y' ELSE 'N'
    END                                                      AS is_last_day_quarter,
    CASE MONTH(calendar_date)
        WHEN 12 THEN 'WINTER' WHEN  1 THEN 'WINTER' WHEN  2 THEN 'WINTER'
        WHEN  3 THEN 'SPRING' WHEN  4 THEN 'SPRING' WHEN  5 THEN 'SPRING'
        WHEN  6 THEN 'SUMMER' WHEN  7 THEN 'SUMMER' WHEN  8 THEN 'SUMMER'
        ELSE 'FALL'
    END                                                      AS season_code
FROM date_spine;


-- #############################################################################
-- OBJECT 3 OF 14: dim_product (basic) — Source: sql2.sql, Section 2
-- Score: 5 | Complexity: Medium
-- Pattern: SCD1 UPDATE + INSERT → MERGE INTO
-- #############################################################################
-- Provenance: sql2.sql lines 229–397
-- Sybase: IF EXISTS/DROP + CREATE + UPDATE...FROM + INSERT...WHERE NOT EXISTS
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] SCD1 UPDATE + INSERT → MERGE INTO ... USING stg_products
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE dim_product (
    product_key         INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    product_code        VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(150)    NOT NULL,
    product_line        VARCHAR(50)     NOT NULL,
    lob_code            VARCHAR(20)     NULL,
    lob_description     VARCHAR(100)    NULL,
    sub_lob_code        VARCHAR(20)     NULL,
    coverage_type       VARCHAR(50)     NULL,
    rating_plan         VARCHAR(30)     NULL,
    min_premium         NUMBER(18,2)    NULL,
    max_coverage        NUMBER(18,2)    NULL,
    is_active           CHAR(1)         NOT NULL DEFAULT 'Y',
    effective_date      DATE            NULL,
    discontinue_date    DATE            NULL,
    dw_insert_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] SCD1 UPDATE + INSERT → MERGE (type-map §6: SCD1)
MERGE INTO dim_product AS tgt
USING stg_products AS src
    ON tgt.product_code = src.product_code
WHEN MATCHED AND (
       tgt.product_name <> src.product_name
    OR tgt.lob_code     <> COALESCE(src.lob_code, tgt.lob_code)
    OR tgt.is_active    <> src.active_flag
) THEN UPDATE SET
    tgt.product_name     = src.product_name,
    tgt.product_line     = COALESCE(src.product_line, 'UNKNOWN'),
    tgt.lob_code         = src.lob_code,
    tgt.lob_description  = CASE src.lob_code
                               WHEN 'AUTO' THEN 'Personal Auto'
                               WHEN 'HOME' THEN 'Homeowners'
                               WHEN 'COMM' THEN 'Commercial Lines'
                               WHEN 'LIFE' THEN 'Life and Annuity'
                               ELSE 'Other'
                           END,
    tgt.sub_lob_code     = src.sub_lob_code,
    tgt.coverage_type    = src.coverage_type,
    tgt.rating_plan      = src.rating_plan,
    tgt.min_premium      = src.min_premium,
    tgt.max_coverage     = src.max_coverage,
    tgt.is_active        = src.active_flag,
    tgt.effective_date   = src.effective_date,
    tgt.discontinue_date = src.discontinue_date,
    tgt.dw_update_ts     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_code, product_name, product_line,
    lob_code, lob_description, sub_lob_code,
    coverage_type, rating_plan, min_premium, max_coverage,
    is_active, effective_date, discontinue_date
)
VALUES (
    src.product_code,
    src.product_name,
    COALESCE(src.product_line, 'UNKNOWN'),
    src.lob_code,
    CASE src.lob_code
        WHEN 'AUTO' THEN 'Personal Auto'
        WHEN 'HOME' THEN 'Homeowners'
        WHEN 'COMM' THEN 'Commercial Lines'
        WHEN 'LIFE' THEN 'Life and Annuity'
        ELSE 'Other'
    END,
    src.sub_lob_code,
    src.coverage_type,
    src.rating_plan,
    src.min_premium,
    src.max_coverage,
    src.active_flag,
    src.effective_date,
    src.discontinue_date
);


-- #############################################################################
-- OBJECT 4 OF 14: dim_product (enhanced) — Source: ETL_Script_02.sql, Section 2
-- Score: 7 | Complexity: Complex
-- Pattern: SCD1 UPDATE + INSERT → MERGE; MONEY → NUMBER(19,4)
-- #############################################################################
-- Provenance: ETL_Script_02.sql lines 129–200
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + UPDATE...FROM + INSERT...WHERE NOT EXISTS
--         with MONEY columns, STR(@@ROWCOUNT, 6) audit PRINT
-- [CONVERT] SCD1 UPDATE + INSERT → MERGE
-- [CONVERT] MONEY → NUMBER(19,4)
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] DECIMAL(10,6) → NUMBER(10,6)
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  STR(@@ROWCOUNT, 6) + PRINT audit lines
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE dim_product_enhanced (
    product_key         INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    product_code        VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(200)    NOT NULL,
    product_line        VARCHAR(50)     NULL,
    lob_code            VARCHAR(10)     NULL,
    lob_description     VARCHAR(100)    NULL,
    coverage_type       VARCHAR(50)     NULL,
    base_rate           NUMBER(10,6)    NULL,
    min_premium         NUMBER(19,4)    NULL,      -- [CONVERT] MONEY → NUMBER(19,4)
    max_coverage        NUMBER(19,4)    NULL,      -- [CONVERT] MONEY → NUMBER(19,4)
    is_commercial       CHAR(1)         NULL,
    is_reinsurable      CHAR(1)         NULL,
    filing_state        CHAR(2)         NULL,
    effective_date      DATE            NULL,
    expiry_date         DATE            NULL,
    dw_created_ts       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_ts       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] SCD1 UPDATE + INSERT → MERGE (type-map §6: SCD1)
MERGE INTO dim_product_enhanced AS tgt
USING stg_products AS src
    ON tgt.product_code = src.product_code
WHEN MATCHED THEN UPDATE SET
    tgt.product_name    = src.product_name,
    tgt.product_line    = src.product_line,
    tgt.lob_code        = src.lob_code,
    tgt.lob_description = src.lob_description,
    tgt.coverage_type   = src.coverage_type,
    tgt.base_rate       = src.base_rate,
    tgt.min_premium     = src.min_premium,
    tgt.max_coverage    = src.max_coverage,
    tgt.is_commercial   = src.is_commercial,
    tgt.is_reinsurable  = src.is_reinsurable,
    tgt.filing_state    = src.filing_state,
    tgt.effective_date  = src.effective_date,
    tgt.expiry_date     = src.expiry_date,
    tgt.dw_updated_ts   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_code, product_name, product_line, lob_code, lob_description,
    coverage_type, base_rate, min_premium, max_coverage,
    is_commercial, is_reinsurable, filing_state, effective_date, expiry_date
)
VALUES (
    src.product_code, src.product_name, src.product_line,
    src.lob_code, src.lob_description,
    src.coverage_type, src.base_rate, src.min_premium, src.max_coverage,
    src.is_commercial, src.is_reinsurable, src.filing_state,
    src.effective_date, src.expiry_date
);


-- #############################################################################
-- OBJECT 5 OF 14: dim_agent (basic) — Source: sql2.sql, Section 3
-- Score: 8 | Complexity: Complex
-- Pattern: SCD2 expire + insert (2-step)
-- #############################################################################
-- Provenance: sql2.sql lines 409–609
-- Sybase: IF EXISTS/DROP + CREATE + UPDATE expire + INSERT new version
--         GETDATE(), CAST(GETDATE() AS DATE), ISNULL, correlated subquery for scd_version
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] DATEADD(DAY, -1, GETDATE()) → DATEADD('DAY', -1, CURRENT_TIMESTAMP())
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE dim_agent (
    agent_key           INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    agent_id            INT             NOT NULL,   -- natural key
    agent_number        VARCHAR(20)     NOT NULL,
    agent_name          VARCHAR(200)    NOT NULL,
    agency_id           INT             NULL,
    agency_name         VARCHAR(200)    NULL,
    license_state       CHAR(2)         NULL,
    license_number      VARCHAR(30)     NULL,
    license_expiry      DATE            NULL,
    region_code         VARCHAR(10)     NULL,
    region_description  VARCHAR(100)    NULL,
    channel_code        VARCHAR(20)     NULL,
    channel_description VARCHAR(100)    NULL,
    is_active           CHAR(1)         NOT NULL DEFAULT 'Y',
    hire_date           DATE            NULL,
    -- SCD2 tracking columns
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE            NULL,       -- NULL = current record
    scd_current_flag    CHAR(1)         NOT NULL DEFAULT 'Y',
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    dw_insert_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- SCD2 Step A: Expire changed agent records
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP(); DATEADD(DAY,-1,...) → DATEADD('DAY',-1,...)
-- [CONVERT] ISNULL(x, default) → COALESCE(x, default)
UPDATE dim_agent AS da
SET
    da.scd_end_date     = DATEADD('DAY', -1, CURRENT_TIMESTAMP())::DATE,
    da.scd_current_flag = 'N',
    da.dw_update_ts     = CURRENT_TIMESTAMP()
FROM stg_agents AS sa
WHERE da.agent_id = sa.agent_id
  AND da.scd_current_flag = 'Y'
  AND (
       da.agency_id      <> COALESCE(sa.agency_id, -1)
    OR da.region_code     <> COALESCE(sa.region_code, 'UNKN')
    OR da.channel_code    <> COALESCE(sa.channel_code, 'UNKN')
    OR da.is_active       <> sa.active_flag
    OR da.license_expiry  <> COALESCE(sa.license_expiry, da.license_expiry)
  );

-- SCD2 Step B: Insert new version for changed agents + brand new agents
-- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
-- [CONVERT] ISNULL((SELECT MAX(...)), 1) → COALESCE((SELECT MAX(...)), 1)
INSERT INTO dim_agent (
    agent_id, agent_number, agent_name,
    agency_id, agency_name,
    license_state, license_number, license_expiry,
    region_code, region_description,
    channel_code, channel_description,
    is_active, hire_date,
    scd_start_date, scd_end_date, scd_current_flag, scd_version
)
SELECT
    sa.agent_id,
    sa.agent_number,
    sa.agent_name,
    sa.agency_id,
    sa.agency_name,
    sa.license_state,
    sa.license_number,
    sa.license_expiry,
    sa.region_code,
    CASE sa.region_code
        WHEN 'NE' THEN 'Northeast'
        WHEN 'SE' THEN 'Southeast'
        WHEN 'MW' THEN 'Midwest'
        WHEN 'SW' THEN 'Southwest'
        WHEN 'WC' THEN 'West Coast'
        ELSE 'Other'
    END,
    sa.channel_code,
    CASE sa.channel_code
        WHEN 'INDEP'   THEN 'Independent Agent'
        WHEN 'CAPTIVE' THEN 'Captive Agent'
        WHEN 'DIRECT'  THEN 'Direct / Online'
        WHEN 'BROKER'  THEN 'Wholesale Broker'
        ELSE 'Unknown'
    END,
    sa.active_flag,
    sa.hire_date,
    CURRENT_DATE(),                   -- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
    NULL,                             -- current record: no end date
    'Y',
    COALESCE((                        -- [CONVERT] ISNULL → COALESCE
        SELECT MAX(x.scd_version) + 1
        FROM dim_agent x WHERE x.agent_id = sa.agent_id
    ), 1)
FROM stg_agents sa
WHERE NOT EXISTS (
    SELECT 1 FROM dim_agent da
    WHERE  da.agent_id         = sa.agent_id
    AND    da.scd_current_flag = 'Y'
);


-- #############################################################################
-- OBJECT 6 OF 14: dim_agent (enhanced) — Source: ETL_Script_02.sql, Section 3
-- Score: 15 | Complexity: HIGH QUARANTINE RISK
-- Pattern: SCD2 + CURSOR for hierarchy path + GOTO + #agent_hierarchy temp
-- #############################################################################
-- Provenance: ETL_Script_02.sql lines 202–395
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + SCD2 expire UPDATE +
--         DECLARE CURSOR FOR + WHILE @@FETCH_STATUS=0 + hierarchy path build +
--         GOTO cleanup_agent label + #agent_hierarchy temp table + @@ERROR +
--         RAISERROR
-- [CONVERT] CURSOR + WHILE @@FETCH_STATUS → set-based CTE (hierarchy path via CASE)
-- [CONVERT] #agent_hierarchy → CREATE OR REPLACE TEMPORARY TABLE
-- [CONVERT] GOTO cleanup_agent → restructured as IF/ELSE with EXCEPTION block
-- [CONVERT] @@FETCH_STATUS → [REMOVE] (no cursor)
-- [CONVERT] @@ERROR + RAISERROR → Snowflake EXCEPTION WHEN OTHER THEN
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  CLOSE/DEALLOCATE cursor
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
--
-- [TODO] QUARANTINE: The CURSOR → CTE conversion replaces row-by-row hierarchy
--        path materialization with a set-based CASE expression. This works for
--        the 3-level hierarchy (parent_agency > agency > agent) shown in source.
--        If deeper hierarchies exist, a RECURSIVE CTE should replace this approach.
--        Manual review: verify hierarchy_level never exceeds 3 in stg_agents.
-- =============================================================================

CREATE OR REPLACE TABLE dim_agent_enhanced (
    agent_key           INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    agent_id            INT             NOT NULL,
    agent_number        VARCHAR(20)     NOT NULL,
    agent_name          VARCHAR(200)    NOT NULL,
    agency_id           INT             NULL,
    agency_name         VARCHAR(200)    NULL,
    parent_agency_id    INT             NULL,
    hierarchy_level     SMALLINT        NULL,
    hierarchy_path      VARCHAR(500)    NULL,      -- Materialized breadcrumb
    license_state       CHAR(2)         NULL,
    license_number      VARCHAR(30)     NULL,
    license_expiry      DATE            NULL,
    region_code         VARCHAR(10)     NULL,
    region_description  VARCHAR(100)    NULL,
    district_code       VARCHAR(10)     NULL,
    channel_code        VARCHAR(20)     NULL,
    channel_description VARCHAR(100)    NULL,
    commission_tier     VARCHAR(10)     NULL,
    active_flag         CHAR(1)         NULL,
    hire_date           DATE            NULL,
    termination_date    DATE            NULL,
    effective_date      DATE            NOT NULL DEFAULT CURRENT_DATE(),
    expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
    is_current          CHAR(1)         NOT NULL DEFAULT 'Y',
    dw_created_ts       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- SCD2 Step 1: Expire changed records
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP(); ISNULL() → COALESCE()
UPDATE dim_agent_enhanced AS d
SET
    d.expiry_date = DATEADD('DAY', -1, CURRENT_TIMESTAMP())::DATE,
    d.is_current  = 'N'
FROM stg_agents AS s
WHERE s.agent_id = d.agent_id
  AND d.is_current = 'Y'
  AND (
       d.agent_name                        <> s.agent_name
    OR COALESCE(d.agency_id, -1)           <> COALESCE(s.agency_id, -1)
    OR COALESCE(d.region_code, '')         <> COALESCE(s.region_code, '')
    OR COALESCE(d.channel_code, '')        <> COALESCE(s.channel_code, '')
    OR COALESCE(d.commission_tier, '')     <> COALESCE(s.commission_tier, '')
    OR d.active_flag                       <> s.active_flag
  );

-- [CONVERT] CURSOR → set-based CTE for hierarchy path + description lookups
-- [CONVERT] #agent_hierarchy → TEMPORARY TABLE populated by CTE
-- [CONVERT] GOTO cleanup_agent → EXCEPTION block wrapping the logic
-- [TODO] QUARANTINE: If hierarchy_level > 3 exists, replace CASE with RECURSIVE CTE:
--        WITH RECURSIVE agent_tree AS (
--            SELECT agent_id, CAST(agent_id AS VARCHAR) AS path, 1 AS lvl
--            FROM stg_agents WHERE parent_agency_id IS NULL
--            UNION ALL
--            SELECT c.agent_id, t.path || '>' || CAST(c.agent_id AS VARCHAR), t.lvl + 1
--            FROM stg_agents c JOIN agent_tree t ON c.parent_agency_id = t.agent_id
--        )
EXECUTE IMMEDIATE $$
BEGIN
    -- Create temp table to hold resolved hierarchy (replaces #agent_hierarchy)
    CREATE OR REPLACE TEMPORARY TABLE tmp_agent_hierarchy (
        agent_id        INT         NOT NULL,
        hierarchy_path  VARCHAR(500) NULL,
        region_desc     VARCHAR(100) NULL,
        channel_desc    VARCHAR(100) NULL
    );

    -- [CONVERT] CURSOR loop → single set-based INSERT with CASE expressions
    -- Builds hierarchy path: PARENT_AGENCY > AGENCY > AGENT
    INSERT INTO tmp_agent_hierarchy (agent_id, hierarchy_path, region_desc, channel_desc)
    SELECT
        s.agent_id,
        -- Materialized path: ROOT > PARENT_AGENCY > AGENCY > AGENT
        CASE
            WHEN s.parent_agency_id IS NULL AND s.agency_id IS NULL
                THEN CAST(s.agent_id AS VARCHAR)
            WHEN s.parent_agency_id IS NULL
                THEN CAST(s.agency_id AS VARCHAR) || '>' || CAST(s.agent_id AS VARCHAR)
            ELSE
                CAST(s.parent_agency_id AS VARCHAR) || '>'
                || CAST(s.agency_id AS VARCHAR) || '>'
                || CAST(s.agent_id AS VARCHAR)
        END AS hierarchy_path,
        -- Region description lookup
        CASE s.region_code
            WHEN 'NE'  THEN 'Northeast'
            WHEN 'SE'  THEN 'Southeast'
            WHEN 'MW'  THEN 'Midwest'
            WHEN 'SW'  THEN 'Southwest'
            WHEN 'W'   THEN 'West'
            WHEN 'NW'  THEN 'Northwest'
            ELSE COALESCE(s.region_code, 'Unknown Region')
        END AS region_desc,
        -- Channel description lookup
        CASE s.channel_code
            WHEN 'INDEPENDENT' THEN 'Independent Agent'
            WHEN 'CAPTIVE'     THEN 'Captive Agent'
            WHEN 'DIRECT'      THEN 'Direct to Consumer'
            WHEN 'BROKER'      THEN 'Wholesale Broker'
            WHEN 'MGA'         THEN 'Managing General Agent'
            ELSE COALESCE(s.channel_code, 'Other')
        END AS channel_desc
    FROM stg_agents s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_agent_enhanced d
        WHERE d.agent_id = s.agent_id AND d.is_current = 'Y'
    );

    -- Insert new / changed SCD2 agent rows using resolved hierarchy
    INSERT INTO dim_agent_enhanced (
        agent_id, agent_number, agent_name, agency_id, agency_name,
        parent_agency_id, hierarchy_level, hierarchy_path,
        license_state, license_number, license_expiry,
        region_code, region_description, district_code,
        channel_code, channel_description, commission_tier,
        active_flag, hire_date, termination_date,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.agent_id, s.agent_number, s.agent_name, s.agency_id, s.agency_name,
        s.parent_agency_id, s.hierarchy_level,
        COALESCE(h.hierarchy_path, CAST(s.agent_id AS VARCHAR)),
        s.license_state, s.license_number, s.license_expiry,
        s.region_code,
        COALESCE(h.region_desc, s.region_code),
        s.district_code,
        s.channel_code,
        COALESCE(h.channel_desc, s.channel_code),
        s.commission_tier,
        s.active_flag, s.hire_date, s.termination_date,
        CURRENT_TIMESTAMP()::DATE, '9999-12-31'::DATE, 'Y'
    FROM stg_agents s
    LEFT JOIN tmp_agent_hierarchy h ON h.agent_id = s.agent_id
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_agent_enhanced d
        WHERE d.agent_id = s.agent_id AND d.is_current = 'Y'
    );

    -- [CONVERT] GOTO cleanup_agent → always cleanup in both success and exception paths
    DROP TABLE IF EXISTS tmp_agent_hierarchy;

EXCEPTION
    -- [CONVERT] @@ERROR <> 0 + RAISERROR → Snowflake EXCEPTION block
    WHEN OTHER THEN
        DROP TABLE IF EXISTS tmp_agent_hierarchy;
        RAISE;   -- Re-raise after cleanup
END;
$$;


-- #############################################################################
-- OBJECT 7 OF 14: dim_customer (basic) — Source: sql2.sql, Section 4
-- Score: 12 | Complexity: Complex
-- Pattern: SCD2 expire + insert with deeply nested CASE expressions
-- #############################################################################
-- Provenance: sql2.sql lines 621–895
-- Sybase: IF EXISTS/DROP + CREATE + UPDATE expire + INSERT with CASE for
--         age_band, gender, marital, state_name, region, credit_band
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] DATEDIFF(YEAR, dob, GETDATE()) → DATEDIFF('YEAR', dob, CURRENT_TIMESTAMP())
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
-- [CONVERT] DATEADD(DAY, -1, GETDATE()) → DATEADD('DAY', -1, CURRENT_TIMESTAMP())
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE dim_customer (
    customer_key        BIGINT          AUTOINCREMENT NOT NULL PRIMARY KEY,
    customer_id         BIGINT          NOT NULL,   -- natural key
    full_name           VARCHAR(200)    NULL,
    first_name          VARCHAR(100)    NULL,
    last_name           VARCHAR(100)    NULL,
    date_of_birth       DATE            NULL,
    age_band            VARCHAR(20)     NULL,
    gender_code         CHAR(1)         NULL,
    gender_description  VARCHAR(10)     NULL,
    marital_status      CHAR(1)         NULL,
    marital_description VARCHAR(20)     NULL,
    city                VARCHAR(100)    NULL,
    state_code          CHAR(2)         NULL,
    state_name          VARCHAR(50)     NULL,
    zip_code            VARCHAR(10)     NULL,
    region              VARCHAR(30)     NULL,
    credit_score        SMALLINT        NULL,
    credit_band         VARCHAR(20)     NULL,
    customer_segment    VARCHAR(30)     NULL,
    acquisition_source  VARCHAR(50)     NULL,
    -- SCD2 tracking columns
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE            NULL,
    scd_current_flag    CHAR(1)         NOT NULL DEFAULT 'Y',
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    dw_insert_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_update_ts        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- SCD2 Step A: Expire records that have changed on tracked attributes
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP(); ISNULL() → COALESCE()
UPDATE dim_customer AS dc
SET
    dc.scd_end_date     = DATEADD('DAY', -1, CURRENT_TIMESTAMP())::DATE,
    dc.scd_current_flag = 'N',
    dc.dw_update_ts     = CURRENT_TIMESTAMP()
FROM stg_customers AS sc
WHERE dc.customer_id = sc.customer_id
  AND dc.scd_current_flag = 'Y'
  AND (
       COALESCE(dc.state_code,       'XX')    <> COALESCE(sc.state_code,       'XX')
    OR COALESCE(dc.zip_code,         '00000') <> COALESCE(sc.zip_code,         '00000')
    OR COALESCE(dc.marital_status,   'X')     <> COALESCE(sc.marital_status,   'X')
    OR COALESCE(dc.credit_score,     0)       <> COALESCE(sc.credit_score,     0)
    OR COALESCE(dc.customer_segment, '')      <> COALESCE(sc.customer_segment, '')
  );

-- SCD2 Step B: Insert new / new-version customer records
-- [CONVERT] DATEDIFF(YEAR, dob, GETDATE()) → DATEDIFF('YEAR', dob, CURRENT_TIMESTAMP())
-- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
INSERT INTO dim_customer (
    customer_id, full_name, first_name, last_name,
    date_of_birth, age_band,
    gender_code, gender_description,
    marital_status, marital_description,
    city, state_code, state_name, zip_code, region,
    credit_score, credit_band,
    customer_segment, acquisition_source,
    scd_start_date, scd_end_date, scd_current_flag, scd_version
)
SELECT
    sc.customer_id,
    LTRIM(RTRIM(COALESCE(sc.first_name, '') || ' ' || COALESCE(sc.last_name, ''))),
    sc.first_name,
    sc.last_name,
    sc.date_of_birth,
    -- Derive age band from DOB
    CASE
        WHEN DATEDIFF('YEAR', sc.date_of_birth, CURRENT_TIMESTAMP()) < 26 THEN '18-25'
        WHEN DATEDIFF('YEAR', sc.date_of_birth, CURRENT_TIMESTAMP()) < 36 THEN '26-35'
        WHEN DATEDIFF('YEAR', sc.date_of_birth, CURRENT_TIMESTAMP()) < 46 THEN '36-45'
        WHEN DATEDIFF('YEAR', sc.date_of_birth, CURRENT_TIMESTAMP()) < 56 THEN '46-55'
        WHEN DATEDIFF('YEAR', sc.date_of_birth, CURRENT_TIMESTAMP()) < 66 THEN '56-65'
        ELSE '65+'
    END,
    sc.gender_code,
    CASE sc.gender_code WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Unknown' END,
    sc.marital_status,
    CASE sc.marital_status
        WHEN 'S' THEN 'Single'   WHEN 'M' THEN 'Married'
        WHEN 'D' THEN 'Divorced' WHEN 'W' THEN 'Widowed'
        ELSE 'Unknown'
    END,
    sc.city,
    sc.state_code,
    -- State name lookup
    CASE sc.state_code
        WHEN 'CA' THEN 'California'  WHEN 'TX' THEN 'Texas'
        WHEN 'FL' THEN 'Florida'     WHEN 'NY' THEN 'New York'
        WHEN 'IL' THEN 'Illinois'    WHEN 'OH' THEN 'Ohio'
        WHEN 'PA' THEN 'Pennsylvania' ELSE sc.state_code
    END,
    LEFT(sc.zip_code, 5),
    -- Region lookup
    CASE sc.state_code
        WHEN 'ME' THEN 'Northeast' WHEN 'NH' THEN 'Northeast'
        WHEN 'VT' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
        WHEN 'RI' THEN 'Northeast' WHEN 'CT' THEN 'Northeast'
        WHEN 'NY' THEN 'Northeast' WHEN 'NJ' THEN 'Northeast'
        WHEN 'PA' THEN 'Northeast'
        WHEN 'FL' THEN 'Southeast' WHEN 'GA' THEN 'Southeast'
        WHEN 'SC' THEN 'Southeast' WHEN 'NC' THEN 'Southeast'
        WHEN 'VA' THEN 'Southeast'
        WHEN 'TX' THEN 'South'     WHEN 'OK' THEN 'South'
        WHEN 'LA' THEN 'South'     WHEN 'AR' THEN 'South'
        WHEN 'CA' THEN 'West'      WHEN 'OR' THEN 'West'
        WHEN 'WA' THEN 'West'      WHEN 'NV' THEN 'West'
        WHEN 'IL' THEN 'Midwest'   WHEN 'OH' THEN 'Midwest'
        WHEN 'MI' THEN 'Midwest'   WHEN 'IN' THEN 'Midwest'
        ELSE 'Other'
    END,
    sc.credit_score,
    CASE
        WHEN sc.credit_score >= 800              THEN 'Exceptional (800+)'
        WHEN sc.credit_score >= 740              THEN 'Very Good (740-799)'
        WHEN sc.credit_score >= 670              THEN 'Good (670-739)'
        WHEN sc.credit_score >= 580              THEN 'Fair (580-669)'
        WHEN sc.credit_score IS NOT NULL         THEN 'Poor (<580)'
        ELSE 'Unknown'
    END,
    sc.customer_segment,
    sc.acquisition_source,
    CURRENT_DATE(),       -- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
    NULL,
    'Y',
    COALESCE((
        SELECT MAX(x.scd_version) + 1
        FROM dim_customer x WHERE x.customer_id = sc.customer_id
    ), 1)
FROM stg_customers sc
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer dc
    WHERE  dc.customer_id      = sc.customer_id
    AND    dc.scd_current_flag = 'Y'
);


-- #############################################################################
-- OBJECT 8 OF 14: dim_customer (enhanced) — Source: ETL_Script_02.sql, Section 4
-- Score: 17 | Complexity: HIGH QUARANTINE RISK
-- Pattern: SCD2 + #changed_customers temp + BEGIN TRAN + @@ERROR + RAISERROR
-- #############################################################################
-- Provenance: ETL_Script_02.sql lines 397–565
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + #changed_customers temp table +
--         INSERT changed IDs + BEGIN TRAN + UPDATE expire + @@ERROR check +
--         ROLLBACK TRAN + RAISERROR + BEGIN TRAN + INSERT new SCD2 rows +
--         @@ERROR + ROLLBACK + RAISERROR + DROP TABLE #changed_customers
-- [CONVERT] #changed_customers → CREATE OR REPLACE TEMPORARY TABLE
-- [CONVERT] BEGIN TRAN / COMMIT / ROLLBACK → Snowflake Scripting EXCEPTION block
-- [CONVERT] @@ERROR <> 0 checks → EXCEPTION WHEN OTHER THEN
-- [CONVERT] RAISERROR('msg', 16, 1) → RAISE (re-raise after cleanup)
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] DATEDIFF(year, dob, GETDATE()) → DATEDIFF('YEAR', dob, CURRENT_TIMESTAMP())
-- [CONVERT] MONEY → NUMBER(19,4)
-- [CONVERT] DECIMAL(5,4) → NUMBER(5,4)
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- [REMOVE]  RETURN (replaced by RAISE)
--
-- [TODO] QUARANTINE: The transactional semantics differ between Sybase and Snowflake.
--        Sybase used named transactions (expire_customers, insert_customers) with
--        per-step ROLLBACK. Snowflake Scripting wraps the entire block; a failure
--        at any point rolls back the anonymous block. Verify that partial-commit
--        behavior is not relied upon downstream (i.e., expiry without insert is not
--        a valid state in the warehouse). If partial commits are needed, split into
--        separate EXECUTE IMMEDIATE blocks.
-- =============================================================================

CREATE OR REPLACE TABLE dim_customer_enhanced (
    customer_key        INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    customer_id         BIGINT          NOT NULL,
    full_name           VARCHAR(205)    NULL,
    date_of_birth       DATE            NULL,
    age_band            VARCHAR(20)     NULL,
    gender_code         CHAR(1)         NULL,
    gender_description  VARCHAR(20)     NULL,
    marital_status      CHAR(1)         NULL,
    marital_description VARCHAR(30)     NULL,
    address_line1       VARCHAR(200)    NULL,
    city                VARCHAR(100)    NULL,
    state_code          CHAR(2)         NULL,
    state_name          VARCHAR(100)    NULL,
    region              VARCHAR(50)     NULL,
    zip_code            VARCHAR(10)     NULL,
    email               VARCHAR(150)    NULL,
    credit_score        SMALLINT        NULL,
    credit_band         VARCHAR(20)     NULL,
    fico_band           VARCHAR(10)     NULL,
    customer_segment    VARCHAR(30)     NULL,
    acquisition_source  VARCHAR(50)     NULL,
    lifetime_value      NUMBER(19,4)    NULL,      -- [CONVERT] MONEY → NUMBER(19,4)
    churn_risk_score    NUMBER(5,4)     NULL,
    is_vip              CHAR(1)         NULL,
    effective_date      DATE            NOT NULL DEFAULT CURRENT_DATE(),
    expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
    is_current          CHAR(1)         NOT NULL DEFAULT 'Y',
    dw_created_ts       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] Entire transactional block (BEGIN TRAN + @@ERROR + RAISERROR + ROLLBACK)
--           → Snowflake Scripting anonymous block with EXCEPTION
EXECUTE IMMEDIATE $$
DECLARE
    v_changed_count INTEGER;
BEGIN
    -- Step 1: Stage changed customers into temp table
    -- [CONVERT] #changed_customers → TEMPORARY TABLE
    CREATE OR REPLACE TEMPORARY TABLE tmp_changed_customers (
        customer_id BIGINT NOT NULL
    );

    INSERT INTO tmp_changed_customers (customer_id)
    SELECT s.customer_id
    FROM stg_customers s
    INNER JOIN dim_customer_enhanced d
        ON d.customer_id = s.customer_id AND d.is_current = 'Y'
    WHERE
        COALESCE(d.state_code, '')          <> COALESCE(s.state_code, '')
        OR COALESCE(d.credit_score, -1)     <> COALESCE(s.credit_score, -1)
        OR COALESCE(d.fico_band, '')        <> COALESCE(s.fico_band, '')
        OR COALESCE(d.customer_segment, '') <> COALESCE(s.customer_segment, '')
        OR COALESCE(d.address_line1, '')    <> COALESCE(s.address_line1, '')
        OR COALESCE(d.email, '')            <> COALESCE(s.email, '')
        OR COALESCE(d.is_vip, 'N')         <> COALESCE(s.is_vip, 'N');

    SELECT COUNT(*) INTO :v_changed_count FROM tmp_changed_customers;

    -- Step 2: Expire changed customers (was BEGIN TRAN expire_customers)
    UPDATE dim_customer_enhanced
    SET expiry_date = DATEADD('DAY', -1, CURRENT_TIMESTAMP())::DATE,
        is_current  = 'N'
    FROM dim_customer_enhanced d
    INNER JOIN tmp_changed_customers c ON c.customer_id = d.customer_id
    WHERE d.is_current = 'Y';

    -- Step 3: Insert new SCD2 rows for changed + genuinely new customers
    -- (was BEGIN TRAN insert_customers)
    INSERT INTO dim_customer_enhanced (
        customer_id, full_name, date_of_birth, age_band,
        gender_code, gender_description, marital_status, marital_description,
        address_line1, city, state_code, state_name, region, zip_code, email,
        credit_score, credit_band, fico_band, customer_segment,
        acquisition_source, lifetime_value, churn_risk_score, is_vip,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.customer_id,
        LTRIM(RTRIM(COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, ''))),
        s.date_of_birth,
        -- Age band
        CASE
            WHEN DATEDIFF('YEAR', s.date_of_birth, CURRENT_TIMESTAMP()) < 25 THEN '18-24'
            WHEN DATEDIFF('YEAR', s.date_of_birth, CURRENT_TIMESTAMP()) < 35 THEN '25-34'
            WHEN DATEDIFF('YEAR', s.date_of_birth, CURRENT_TIMESTAMP()) < 45 THEN '35-44'
            WHEN DATEDIFF('YEAR', s.date_of_birth, CURRENT_TIMESTAMP()) < 55 THEN '45-54'
            WHEN DATEDIFF('YEAR', s.date_of_birth, CURRENT_TIMESTAMP()) < 65 THEN '55-64'
            WHEN s.date_of_birth IS NOT NULL                                  THEN '65+'
            ELSE 'UNKNOWN'
        END,
        s.gender_code,
        CASE s.gender_code WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Not Stated' END,
        s.marital_status,
        CASE s.marital_status
            WHEN 'S' THEN 'Single'   WHEN 'M' THEN 'Married'
            WHEN 'D' THEN 'Divorced' WHEN 'W' THEN 'Widowed'
            ELSE 'Unknown'
        END,
        s.address_line1, s.city,
        s.state_code,
        -- State name lookup (abbreviated; full 50-state table in production)
        CASE s.state_code
            WHEN 'AL' THEN 'Alabama'     WHEN 'AK' THEN 'Alaska'
            WHEN 'AZ' THEN 'Arizona'     WHEN 'AR' THEN 'Arkansas'
            WHEN 'CA' THEN 'California'  WHEN 'CO' THEN 'Colorado'
            WHEN 'CT' THEN 'Connecticut' WHEN 'FL' THEN 'Florida'
            WHEN 'GA' THEN 'Georgia'     WHEN 'IL' THEN 'Illinois'
            WHEN 'NY' THEN 'New York'    WHEN 'TX' THEN 'Texas'
            ELSE COALESCE(s.state_code, 'Unknown')
        END,
        -- Region lookup
        CASE s.state_code
            WHEN 'ME' THEN 'Northeast' WHEN 'NH' THEN 'Northeast'
            WHEN 'VT' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
            WHEN 'RI' THEN 'Northeast' WHEN 'CT' THEN 'Northeast'
            WHEN 'NY' THEN 'Northeast' WHEN 'NJ' THEN 'Northeast'
            WHEN 'PA' THEN 'Northeast'
            WHEN 'FL' THEN 'Southeast' WHEN 'GA' THEN 'Southeast'
            WHEN 'SC' THEN 'Southeast' WHEN 'NC' THEN 'Southeast'
            WHEN 'TX' THEN 'South'     WHEN 'CA' THEN 'West'
            ELSE 'Other'
        END,
        s.zip_code, s.email,
        s.credit_score, s.fico_band, s.fico_band,
        s.customer_segment, s.acquisition_source,
        s.lifetime_value, s.churn_risk_score, s.is_vip,
        CURRENT_TIMESTAMP()::DATE, '9999-12-31'::DATE, 'Y'
    FROM stg_customers s
    WHERE s.customer_id IN (SELECT customer_id FROM tmp_changed_customers)
       OR NOT EXISTS (
           SELECT 1 FROM dim_customer_enhanced d
           WHERE d.customer_id = s.customer_id
       );

    -- Cleanup temp table
    DROP TABLE IF EXISTS tmp_changed_customers;

EXCEPTION
    -- [CONVERT] @@ERROR + RAISERROR + ROLLBACK → EXCEPTION block
    -- Snowflake auto-rolls back the anonymous block on unhandled exception
    WHEN OTHER THEN
        DROP TABLE IF EXISTS tmp_changed_customers;
        RAISE;  -- Re-raise original error after cleanup
END;
$$;


-- #############################################################################
-- OBJECT 9 OF 14: dim_territory (enhanced) — Source: ETL_Script_02.sql, Section 5
-- Score: 8 | Complexity: Complex
-- Pattern: SCD1 UPDATE + INSERT → MERGE; COMPUTE BY → [REMOVE]
-- #############################################################################
-- Provenance: ETL_Script_02.sql lines 567–647
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + UPDATE...FROM + INSERT +
--         COMPUTE BY audit query + UPDATE denorm count
-- [CONVERT] SCD1 UPDATE + INSERT → MERGE
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  COMPUTE BY audit query (Sybase-specific; no Snowflake equivalent)
--           Replace with: SELECT region_code, COUNT(*) ... GROUP BY region_code
--           or: COUNT(*) OVER (PARTITION BY region_code) as window function
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- =============================================================================

CREATE OR REPLACE TABLE dim_territory (
    territory_key           INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
    territory_code          VARCHAR(10)     NOT NULL,
    territory_name          VARCHAR(100)    NOT NULL,
    state_code              CHAR(2)         NOT NULL,
    state_name              VARCHAR(100)    NULL,
    region_code             VARCHAR(10)     NOT NULL,
    region_name             VARCHAR(100)    NULL,
    zone_code               VARCHAR(10)     NULL,
    zone_name               VARCHAR(100)    NULL,
    catastrophe_zone        VARCHAR(10)     NULL,
    wind_pool_flag          CHAR(1)         NULL,
    territory_count_in_region INT           NULL,
    active_flag             CHAR(1)         NULL,
    dw_created_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] SCD1 UPDATE + INSERT → MERGE (type-map §6: SCD1)
MERGE INTO dim_territory AS tgt
USING stg_territories AS src
    ON tgt.territory_code = src.territory_code
WHEN MATCHED THEN UPDATE SET
    tgt.territory_name   = src.territory_name,
    tgt.state_code       = src.state_code,
    tgt.region_code      = src.region_code,
    tgt.region_name      = src.region_name,
    tgt.zone_code        = src.zone_code,
    tgt.zone_name        = src.zone_name,
    tgt.catastrophe_zone = src.catastrophe_zone,
    tgt.wind_pool_flag   = src.wind_pool_flag,
    tgt.active_flag      = src.active_flag,
    tgt.dw_updated_ts    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    territory_code, territory_name, state_code, region_code, region_name,
    zone_code, zone_name, catastrophe_zone, wind_pool_flag, active_flag
)
VALUES (
    src.territory_code, src.territory_name, src.state_code,
    src.region_code, src.region_name,
    src.zone_code, src.zone_name, src.catastrophe_zone,
    src.wind_pool_flag, src.active_flag
);

-- [REMOVE] COMPUTE BY audit query — Sybase-specific, no direct Snowflake equivalent.
-- Replaced by standard GROUP BY query for territory audit:
--   SELECT region_code,
--          COUNT(*)                                          AS territory_count,
--          SUM(CASE WHEN wind_pool_flag = 'Y' THEN 1 ELSE 0 END) AS wind_pool_count
--   FROM dim_territory
--   WHERE active_flag = 'Y'
--   GROUP BY region_code
--   ORDER BY region_code;
-- Or as window function on dim_territory:
--   COUNT(*) OVER (PARTITION BY region_code) AS territory_count_in_region

-- Update territory count denormalization (unchanged logic, Snowflake-compatible)
UPDATE dim_territory AS d
SET d.territory_count_in_region = rc.cnt
FROM (
    SELECT region_code, COUNT(*) AS cnt
    FROM dim_territory
    WHERE active_flag = 'Y'
    GROUP BY region_code
) AS rc
WHERE rc.region_code = d.region_code;


-- #############################################################################
-- OBJECT 10 OF 14: fact_policy (basic) — Source: sql3.sql, Section 1
-- Score: 7 | Complexity: Complex
-- Pattern: DDL + INSERT + UPDATE with date key conversion
-- #############################################################################
-- Provenance: sql3.sql lines 31–231
-- Sybase: IF EXISTS/DROP + CREATE + INSERT...FROM with CONVERT date keys +
--         UPDATE...FROM for changed policies
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] DATEDIFF(DAY, a, b) → DATEDIFF('DAY', a, b)
-- [CONVERT] CAST(x AS DECIMAL(10,4)) → CAST(x AS NUMBER(10,4))
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE fact_policy (
    policy_fact_key         BIGINT          AUTOINCREMENT NOT NULL PRIMARY KEY,
    -- Dimension keys
    policy_id               BIGINT          NOT NULL,   -- degenerate dim
    customer_key            BIGINT          NOT NULL,
    agent_key               INT             NOT NULL,
    product_key             INT             NOT NULL,
    effective_date_key      INT             NOT NULL,
    expiry_date_key         INT             NOT NULL,
    -- Degenerate dimensions
    policy_number           VARCHAR(30)     NOT NULL,
    status_code             CHAR(2)         NOT NULL,
    territory_code          VARCHAR(10)     NULL,
    -- Measures
    annual_premium          NUMBER(18,2)    NULL,
    coverage_amount         NUMBER(18,2)    NULL,
    policy_term_days        INT             NULL,
    earned_premium_amount   NUMBER(18,2)    NULL,   -- derived
    -- Audit
    dw_insert_ts            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_batch_id             INT             NULL
);

-- Populate fact_policy by resolving surrogate keys
INSERT INTO fact_policy (
    policy_id,
    customer_key, agent_key, product_key,
    effective_date_key, expiry_date_key,
    policy_number, status_code, territory_code,
    annual_premium, coverage_amount, policy_term_days,
    earned_premium_amount,
    dw_batch_id
)
SELECT
    sp.policy_id,
    -- Customer dimension key (current record)
    COALESCE(dc.customer_key, -1),               -- -1 = 'Unknown' member
    COALESCE(da.agent_key,    -1),
    COALESCE(dp.product_key,  -1),
    -- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
    TO_NUMBER(TO_CHAR(sp.effective_date, 'YYYYMMDD')),
    TO_NUMBER(TO_CHAR(sp.expiry_date,    'YYYYMMDD')),
    sp.policy_number,
    sp.status_code,
    sp.territory_code,
    sp.annual_premium,
    sp.coverage_amount,
    DATEDIFF('DAY', sp.effective_date, sp.expiry_date),
    -- Earned premium: prorate for non-full-year terms
    ROUND(
        sp.annual_premium *
        (CAST(DATEDIFF('DAY', sp.effective_date, sp.expiry_date) AS NUMBER(10,4)) / 365.0),
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
WHERE NOT EXISTS (
    SELECT 1 FROM fact_policy fp
    WHERE  fp.policy_id = sp.policy_id
);

-- Update existing fact rows where staging has newer data
-- [CONVERT] Snowflake UPDATE...FROM syntax
UPDATE fact_policy AS fp
SET
    fp.status_code           = sp.status_code,
    fp.annual_premium        = sp.annual_premium,
    fp.coverage_amount       = sp.coverage_amount,
    fp.earned_premium_amount = ROUND(
        sp.annual_premium *
        (CAST(DATEDIFF('DAY', sp.effective_date, sp.expiry_date) AS NUMBER(10,4)) / 365.0),
        2
    ),
    fp.dw_insert_ts          = CURRENT_TIMESTAMP()
FROM stg_policies AS sp
WHERE fp.policy_id = sp.policy_id
  AND (
       fp.status_code    <> sp.status_code
    OR fp.annual_premium <> COALESCE(sp.annual_premium, fp.annual_premium)
  );


-- #############################################################################
-- OBJECT 11 OF 14: fact_claims (basic) — Source: sql3.sql, Section 2
-- Score: 8 | Complexity: Complex
-- Pattern: DDL + INSERT + UPDATE with date key conversion and loss ratio calc
-- #############################################################################
-- Provenance: sql3.sql lines 243–483
-- Sybase: IF EXISTS/DROP + CREATE + INSERT with CONVERT date keys, CASE for
--         closed_date_key NULL, DATEDIFF, ISNULL, loss ratio join to fact_policy +
--         UPDATE for refreshing open claims
-- [CONVERT] IF EXISTS/DROP → CREATE OR REPLACE
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] DATEDIFF(DAY, a, b) → DATEDIFF('DAY', a, b)
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] IDENTITY(1,1) → AUTOINCREMENT
-- [CONVERT] DECIMAL(18,2) → NUMBER(18,2); DECIMAL(10,4) → NUMBER(10,4)
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  GO batch separators
-- =============================================================================

CREATE OR REPLACE TABLE fact_claims (
    claim_fact_key              BIGINT          AUTOINCREMENT NOT NULL PRIMARY KEY,
    -- Dimension keys
    claim_id                    BIGINT          NOT NULL,   -- degenerate
    policy_id                   BIGINT          NOT NULL,   -- degenerate
    customer_key                BIGINT          NOT NULL,
    product_key                 INT             NOT NULL,
    incident_date_key           INT             NOT NULL,
    reported_date_key           INT             NOT NULL,
    closed_date_key             INT             NULL,
    -- Degenerate dimensions
    claim_number                VARCHAR(30)     NOT NULL,
    claim_type_code             VARCHAR(20)     NOT NULL,
    status_code                 VARCHAR(10)     NOT NULL,
    fault_indicator             CHAR(1)         NULL,
    litigation_flag             CHAR(1)         NULL,
    -- Measures
    total_incurred              NUMBER(18,2)    NULL,
    total_paid                  NUMBER(18,2)    NULL,
    total_reserved              NUMBER(18,2)    NULL,
    outstanding_reserve         NUMBER(18,2)    NULL,   -- derived
    claim_report_lag_days       INT             NULL,   -- derived
    claim_settlement_days       INT             NULL,   -- derived (null if open)
    loss_ratio_pct              NUMBER(10,4)    NULL,   -- derived vs policy premium
    -- Audit
    dw_insert_ts                TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_batch_id                 INT             NULL
);

INSERT INTO fact_claims (
    claim_id, policy_id,
    customer_key, product_key,
    incident_date_key, reported_date_key, closed_date_key,
    claim_number, claim_type_code, status_code,
    fault_indicator, litigation_flag,
    total_incurred, total_paid, total_reserved,
    outstanding_reserve,
    claim_report_lag_days, claim_settlement_days,
    loss_ratio_pct,
    dw_batch_id
)
SELECT
    sc.claim_id,
    sc.policy_id,
    COALESCE(dc.customer_key, -1),
    COALESCE(dp.product_key,  -1),
    -- [CONVERT] date key conversion
    TO_NUMBER(TO_CHAR(sc.incident_date, 'YYYYMMDD')),
    TO_NUMBER(TO_CHAR(sc.reported_date, 'YYYYMMDD')),
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
    DATEDIFF('DAY', sc.incident_date, sc.reported_date),
    -- Settlement days: null if still open
    CASE WHEN sc.closed_date IS NOT NULL
         THEN DATEDIFF('DAY', sc.reported_date, sc.closed_date)
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
-- [CONVERT] Snowflake UPDATE...FROM syntax
UPDATE fact_claims AS fc
SET
    fc.status_code           = sc.status_code,
    fc.total_incurred        = COALESCE(sc.total_incurred, 0),
    fc.total_paid            = COALESCE(sc.total_paid,     0),
    fc.total_reserved        = COALESCE(sc.total_reserved, 0),
    fc.outstanding_reserve   = COALESCE(sc.total_reserved, 0) - COALESCE(sc.total_paid, 0),
    fc.closed_date_key       = CASE WHEN sc.closed_date IS NOT NULL
                                    THEN TO_NUMBER(TO_CHAR(sc.closed_date, 'YYYYMMDD'))
                                    ELSE NULL END,
    fc.claim_settlement_days = CASE WHEN sc.closed_date IS NOT NULL
                                    THEN DATEDIFF('DAY', sc.reported_date, sc.closed_date)
                                    ELSE NULL END,
    fc.litigation_flag       = sc.litigation_flag,
    fc.dw_insert_ts          = CURRENT_TIMESTAMP()
FROM stg_claims AS sc
WHERE fc.claim_id = sc.claim_id
  AND (
       fc.status_code    <> sc.status_code
    OR fc.total_paid      < COALESCE(sc.total_paid,     0)
    OR fc.total_reserved <> COALESCE(sc.total_reserved, 0)
  );


-- #############################################################################
-- OBJECT 12 OF 14: fact_policy (enhanced) — Source: ETL_Script_03.sql, Section 1
-- Score: 10 | Complexity: Complex
-- Pattern: #fact_policy_stage temp + BEGIN TRAN + @@ERROR + surrogate key resolution
-- #############################################################################
-- Provenance: ETL_Script_03.sql lines 9–223
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + #fact_policy_stage temp table +
--         INSERT staging with surrogate key lookups + MISSING_KEYS validation +
--         audit log + default key assignment + BEGIN TRAN UPDATE + @@ERROR +
--         ROLLBACK + RAISERROR + BEGIN TRAN INSERT + @@ERROR + ROLLBACK +
--         DROP TABLE #fact_policy_stage
-- [CONVERT] #fact_policy_stage → TEMPORARY TABLE
-- [CONVERT] BEGIN TRAN / COMMIT / ROLLBACK + @@ERROR → Snowflake EXCEPTION block
-- [CONVERT] RAISERROR → RAISE
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] CONVERT(VARCHAR, n) → CAST(n AS VARCHAR)
-- [CONVERT] DATEDIFF(day, a, b) → DATEDIFF('DAY', a, b)
-- [CONVERT] CAST(x AS DECIMAL(10,4)) → CAST(x AS NUMBER(10,4))
-- [CONVERT] DECIMAL(18,2) → NUMBER(18,2)
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- [REMOVE]  RETURN (replaced by RAISE in exception)
-- =============================================================================

CREATE OR REPLACE TABLE fact_policy_enhanced (
    policy_key              INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    annual_premium          NUMBER(18,2)    NULL,
    coverage_amount         NUMBER(18,2)    NULL,
    earned_premium_amount   NUMBER(18,2)    NULL,
    policy_term_days        INT             NULL,
    renewal_count           SMALLINT        NULL,       -- [CONVERT] TINYINT → SMALLINT
    cancellation_reason     VARCHAR(50)     NULL,
    is_reinsured            CHAR(1)         NULL DEFAULT 'N',
    reinsurance_premium_ceded NUMBER(18,2)  NULL,
    dw_created_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] Entire multi-step staging + transactional load → Snowflake Scripting block
EXECUTE IMMEDIATE $$
DECLARE
    v_unresolved INTEGER;
BEGIN
    -- Step 1: Create staging temp table for surrogate key resolution
    -- [CONVERT] #fact_policy_stage → TEMPORARY TABLE
    CREATE OR REPLACE TEMPORARY TABLE tmp_fact_policy_stage (
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
        annual_premium          NUMBER(18,2)    NULL,
        coverage_amount         NUMBER(18,2)    NULL,
        earned_premium_amount   NUMBER(18,2)    NULL,
        policy_term_days        INT             NULL,
        renewal_count           SMALLINT        NULL,
        cancellation_reason     VARCHAR(50)     NULL,
        key_resolve_status      VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
    );

    -- Populate staging with surrogate key lookups
    INSERT INTO tmp_fact_policy_stage (
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
            WHEN DATEDIFF('DAY', p.effective_date, p.expiry_date) > 0
            THEN ROUND(
                p.annual_premium
                * CAST(
                    CASE WHEN CURRENT_TIMESTAMP() >= p.expiry_date
                         THEN DATEDIFF('DAY', p.effective_date, p.expiry_date)
                         ELSE DATEDIFF('DAY', p.effective_date, CURRENT_TIMESTAMP())
                    END AS NUMBER(10,4))
                / DATEDIFF('DAY', p.effective_date, p.expiry_date),
                2)
            ELSE p.annual_premium
        END,
        DATEDIFF('DAY', p.effective_date, p.expiry_date),
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
    LEFT JOIN dim_customer_enhanced dc
        ON dc.customer_id = p.customer_id AND dc.is_current = 'Y'
    LEFT JOIN dim_agent_enhanced da
        ON da.agent_id    = p.agent_id    AND da.is_current = 'Y'
    LEFT JOIN dim_product_enhanced dp
        ON dp.product_code = p.product_code
    LEFT JOIN dim_territory dt
        ON dt.territory_code = p.territory_code AND dt.active_flag = 'Y'
    LEFT JOIN dim_date dd_eff
        ON dd_eff.calendar_date = p.effective_date
    LEFT JOIN dim_date dd_exp
        ON dd_exp.calendar_date = p.expiry_date;

    -- Validate key resolution — flag records with unresolved surrogates
    UPDATE tmp_fact_policy_stage
    SET key_resolve_status = 'MISSING_KEYS'
    WHERE customer_key IS NULL
       OR agent_key    IS NULL
       OR product_key  IS NULL
       OR effective_date_key IS NULL
       OR expiry_date_key    IS NULL;

    SELECT COUNT(*) INTO :v_unresolved
    FROM tmp_fact_policy_stage
    WHERE key_resolve_status = 'MISSING_KEYS';

    IF (:v_unresolved > 0) THEN
        -- Log to audit (if etl_audit_log exists)
        INSERT INTO etl_audit_log (
            script_name, object_name, check_type, status, row_count, error_message, run_ts
        )
        VALUES (
            'etl_03_load_facts', 'fact_policy_enhanced', 'SURROGATE_KEY_RESOLVE',
            'WARNING', :v_unresolved,
            CAST(:v_unresolved AS VARCHAR) || ' policies had unresolved surrogate keys - using defaults',
            CURRENT_TIMESTAMP()
        );

        -- Assign default dimension key (-1 = Unknown member)
        UPDATE tmp_fact_policy_stage
        SET
            customer_key       = COALESCE(customer_key, -1),
            agent_key          = COALESCE(agent_key, -1),
            product_key        = COALESCE(product_key, -1),
            effective_date_key = COALESCE(effective_date_key, 19000101),
            expiry_date_key    = COALESCE(expiry_date_key, 99991231),
            key_resolve_status = 'DEFAULTED'
        WHERE key_resolve_status = 'MISSING_KEYS';
    END IF;

    -- Step 2: UPDATE existing policies
    -- [CONVERT] BEGIN TRAN + @@ERROR → within EXCEPTION block scope
    UPDATE fact_policy_enhanced
    SET
        status_code           = s.status_code,
        annual_premium        = s.annual_premium,
        coverage_amount       = s.coverage_amount,
        earned_premium_amount = s.earned_premium_amount,
        cancellation_reason   = s.cancellation_reason,
        renewal_count         = s.renewal_count,
        customer_key          = s.customer_key,
        agent_key             = s.agent_key,
        dw_updated_ts         = CURRENT_TIMESTAMP()
    FROM fact_policy_enhanced fp
    INNER JOIN tmp_fact_policy_stage s ON s.policy_id = fp.policy_id;

    -- Step 3: INSERT new policies
    INSERT INTO fact_policy_enhanced (
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
    FROM tmp_fact_policy_stage s
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_policy_enhanced fp WHERE fp.policy_id = s.policy_id
    );

    -- Cleanup
    DROP TABLE IF EXISTS tmp_fact_policy_stage;

EXCEPTION
    -- [CONVERT] @@ERROR + RAISERROR + ROLLBACK → EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS tmp_fact_policy_stage;
        RAISE;
END;
$$;


-- #############################################################################
-- OBJECT 13 OF 14: fact_claims (enhanced) — Source: ETL_Script_03.sql, Section 2
-- Score: 12 | Complexity: QUARANTINE CANDIDATE
-- Pattern: INSERT...EXEC sp_validate_claims → direct SELECT + inline CTE
-- #############################################################################
-- Provenance: ETL_Script_03.sql lines 225–391
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + #validated_claims temp +
--         INSERT...EXEC sp_validate_claims (Sybase-specific pattern) +
--         @@ERROR check + UPDATE existing claims + INSERT new claims +
--         UPDATE loss ratio + @@ERROR + DROP TABLE #validated_claims
-- [CONVERT] INSERT...EXEC sp_validate_claims → [TODO] replaced with direct SELECT
--           from stg_claims with inline validation CTE. The procedure's DQ logic
--           must be manually ported into the CTE.
-- [CONVERT] #validated_claims → TEMPORARY TABLE
-- [CONVERT] CONVERT(INT, CONVERT(VARCHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] DATEDIFF(day, a, b) → DATEDIFF('DAY', a, b)
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] DECIMAL(18,2) → NUMBER(18,2); DECIMAL(10,4) → NUMBER(10,4)
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- [REMOVE]  RETURN
--
-- [TODO] QUARANTINE: INSERT...EXEC sp_validate_claims has NO direct Snowflake equivalent.
--        The replacement below uses a direct SELECT from stg_claims with a placeholder
--        validation CTE. You MUST:
--        1. Obtain the source code of sp_validate_claims from Sybase
--        2. Port its validation logic (DQ rules, filtering) into the CTE below
--        3. Verify output schema matches the original procedure's result set
--        4. Test row counts against a known good Sybase run
--        If sp_validate_claims cannot be decomposed, consider creating a Snowflake
--        stored procedure (CREATE OR REPLACE PROCEDURE sp_validate_claims(...))
--        and calling it via CALL, populating the temp table with a separate INSERT.
-- =============================================================================

CREATE OR REPLACE TABLE fact_claims_enhanced (
    claim_key               INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    total_incurred          NUMBER(18,2)    NULL,
    total_paid              NUMBER(18,2)    NULL,
    total_reserved          NUMBER(18,2)    NULL,
    salvage_amount          NUMBER(18,2)    NULL,
    subrogation_amount      NUMBER(18,2)    NULL,
    outstanding_reserve     NUMBER(18,2)    NULL,
    claim_report_lag_days   INT             NULL,
    claim_settlement_days   INT             NULL,
    loss_ratio_pct          NUMBER(10,4)    NULL,
    dw_created_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- [CONVERT] INSERT...EXEC + multi-step load → Snowflake Scripting block
-- [TODO] QUARANTINE: The CTE 'validated_claims' below is a PLACEHOLDER.
--        Port sp_validate_claims DQ logic here. Current implementation passes
--        all stg_claims rows through as 'VALID' — equivalent to @include_invalid=0
--        only if the procedure's sole logic was NULL/empty filtering.
EXECUTE IMMEDIATE $$
BEGIN
    -- [CONVERT] #validated_claims → TEMPORARY TABLE
    -- [TODO] Replace this CTE-based population with actual sp_validate_claims logic
    CREATE OR REPLACE TEMPORARY TABLE tmp_validated_claims AS
    SELECT
        s.claim_id,
        s.claim_number,
        s.policy_id,
        s.customer_id,
        s.incident_date,
        s.reported_date,
        s.closed_date,
        s.claim_type_code,
        s.sub_type_code,
        s.status_code,
        s.fault_indicator,
        s.litigation_flag,
        s.catastrophe_code,
        s.reinsurance_flag,
        s.total_incurred,
        s.total_paid,
        s.total_reserved,
        s.salvage_amount,
        s.subrogation_amount,
        'VALID' AS validation_status
    FROM stg_claims s
    WHERE s.claim_id        IS NOT NULL
      AND s.claim_number    IS NOT NULL
      AND s.policy_id       IS NOT NULL
      AND s.incident_date   IS NOT NULL
      AND s.reported_date   IS NOT NULL
      AND s.claim_type_code IS NOT NULL
      AND s.status_code     IS NOT NULL;

    -- Update existing claims (status, reserves, paid amounts change frequently)
    UPDATE fact_claims_enhanced
    SET
        status_code           = v.status_code,
        total_paid            = v.total_paid,
        total_reserved        = v.total_reserved,
        salvage_amount        = v.salvage_amount,
        subrogation_amount    = v.subrogation_amount,
        outstanding_reserve   = COALESCE(v.total_reserved, 0) - COALESCE(v.total_paid, 0),
        closed_date_key       = CASE
            WHEN v.closed_date IS NOT NULL
            THEN TO_NUMBER(TO_CHAR(v.closed_date, 'YYYYMMDD'))
            ELSE NULL
        END,
        claim_settlement_days = CASE
            WHEN v.closed_date IS NOT NULL
            THEN DATEDIFF('DAY', v.reported_date, v.closed_date)
            ELSE NULL
        END,
        dw_updated_ts         = CURRENT_TIMESTAMP()
    FROM fact_claims_enhanced fc
    INNER JOIN tmp_validated_claims v ON v.claim_id = fc.claim_id;

    -- Insert new claims
    INSERT INTO fact_claims_enhanced (
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
        COALESCE(dc.customer_key, -1),
        COALESCE(fp.product_key,  -1),
        -- [CONVERT] CONVERT(INT, CONVERT(VARCHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
        COALESCE(TO_NUMBER(TO_CHAR(v.incident_date,  'YYYYMMDD')), 19000101),
        COALESCE(TO_NUMBER(TO_CHAR(v.reported_date,  'YYYYMMDD')), 19000101),
        CASE WHEN v.closed_date IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(v.closed_date, 'YYYYMMDD'))
             ELSE NULL END,
        v.claim_type_code, v.sub_type_code, v.status_code,
        v.fault_indicator, v.litigation_flag, v.catastrophe_code, v.reinsurance_flag,
        v.total_incurred, v.total_paid, v.total_reserved,
        v.salvage_amount, v.subrogation_amount,
        COALESCE(v.total_reserved, 0) - COALESCE(v.total_paid, 0),
        DATEDIFF('DAY', v.incident_date, v.reported_date),
        CASE WHEN v.closed_date IS NOT NULL
             THEN DATEDIFF('DAY', v.reported_date, v.closed_date)
             ELSE NULL END
    FROM tmp_validated_claims v
    LEFT JOIN dim_customer_enhanced dc
        ON dc.customer_id = v.customer_id AND dc.is_current = 'Y'
    LEFT JOIN fact_policy_enhanced fp
        ON fp.policy_id   = v.policy_id
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_claims_enhanced fc WHERE fc.claim_id = v.claim_id
    );

    -- Update loss ratio on newly inserted and updated claims
    UPDATE fact_claims_enhanced
    SET loss_ratio_pct = CASE
        WHEN COALESCE(fp.annual_premium, 0) > 0
        THEN ROUND(COALESCE(fc.total_incurred, 0) / fp.annual_premium * 100.0, 4)
        ELSE NULL END
    FROM fact_claims_enhanced fc
    INNER JOIN fact_policy_enhanced fp ON fp.policy_id = fc.policy_id;

    -- Cleanup
    DROP TABLE IF EXISTS tmp_validated_claims;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS tmp_validated_claims;
        RAISE;
END;
$$;


-- #############################################################################
-- OBJECT 14 OF 14: fact_reinsurance (enhanced) — Source: ETL_Script_03.sql, Section 3
-- Score: 7 | Complexity: Complex
-- Pattern: New fact table DDL + INSERT with date key conversion
-- #############################################################################
-- Provenance: ETL_Script_03.sql lines 393–455
-- Sybase: IF NOT EXISTS + CREATE IN DBASPACE + INSERT with CONVERT date keys,
--         ISNULL, CASE for cession_type
-- [CONVERT] IF NOT EXISTS + CREATE → CREATE OR REPLACE
-- [CONVERT] CONVERT(INT, CONVERT(VARCHAR(8), date, 112)) → TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- [CONVERT] TINYINT → SMALLINT
-- [CONVERT] IDENTITY → AUTOINCREMENT
-- [CONVERT] DECIMAL(18,2) → NUMBER(18,2); DECIMAL(7,4) → NUMBER(7,4)
-- [CONVERT] DATETIME → TIMESTAMP_NTZ
-- [REMOVE]  IN DBASPACE
-- [REMOVE]  GO batch separators
-- [REMOVE]  PRINT statements
-- =============================================================================

CREATE OR REPLACE TABLE fact_reinsurance (
    reinsurance_key         INT             AUTOINCREMENT NOT NULL PRIMARY KEY,
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
    layer_number            SMALLINT        NOT NULL,       -- [CONVERT] TINYINT → SMALLINT
    cedant_amount           NUMBER(18,2)    NOT NULL,
    reinsurer_share_pct     NUMBER(7,4)     NOT NULL,
    reinsurer_amount        NUMBER(18,2)    NOT NULL,
    retention_amount        NUMBER(18,2)    NULL,
    reinstatement_flag      CHAR(1)         NULL,
    cession_type            VARCHAR(10)     NULL,           -- PREMIUM or LOSS
    dw_created_ts           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

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
    COALESCE(dp.product_key, -1),
    -- [CONVERT] date key conversion
    COALESCE(TO_NUMBER(TO_CHAR(r.period_start, 'YYYYMMDD')), 19000101),
    COALESCE(TO_NUMBER(TO_CHAR(r.period_end,   'YYYYMMDD')), 19000101),
    CASE WHEN r.settlement_date IS NOT NULL
         THEN TO_NUMBER(TO_CHAR(r.settlement_date, 'YYYYMMDD'))
         ELSE NULL END,
    r.reinsurer_code, r.layer_number,
    r.cedant_amount, r.reinsurer_share_pct, r.reinsurer_amount,
    r.retention_amount, r.reinstatement_flag,
    CASE WHEN r.claim_id IS NULL THEN 'PREMIUM' ELSE 'LOSS' END
FROM stg_reinsurance r
LEFT JOIN fact_policy_enhanced fp ON fp.policy_id = r.policy_id
LEFT JOIN dim_product_enhanced dp ON dp.product_key = fp.product_key
WHERE NOT EXISTS (
    SELECT 1 FROM fact_reinsurance fr WHERE fr.cession_id = r.cession_id
);


-- =============================================================================
-- END OF COMPLEX TIER: Dimensions & Fact Tables
-- =============================================================================
-- Objects converted: 14
--   1.  dim_date             (basic)     — sql2.sql         — Score 8
--   2.  dim_date_enhanced    (enhanced)  — ETL_Script_02    — Score 10
--   3.  dim_product          (basic)     — sql2.sql         — Score 5
--   4.  dim_product_enhanced (enhanced)  — ETL_Script_02    — Score 7
--   5.  dim_agent            (basic)     — sql2.sql         — Score 8
--   6.  dim_agent_enhanced   (enhanced)  — ETL_Script_02    — Score 15  [QUARANTINE]
--   7.  dim_customer         (basic)     — sql2.sql         — Score 12
--   8.  dim_customer_enhanced(enhanced)  — ETL_Script_02    — Score 17  [QUARANTINE]
--   9.  dim_territory        (enhanced)  — ETL_Script_02    — Score 8
--  10.  fact_policy          (basic)     — sql3.sql         — Score 7
--  11.  fact_claims          (basic)     — sql3.sql         — Score 8
--  12.  fact_policy_enhanced (enhanced)  — ETL_Script_03    — Score 10
--  13.  fact_claims_enhanced (enhanced)  — ETL_Script_03    — Score 12  [QUARANTINE]
--  14.  fact_reinsurance     (enhanced)  — ETL_Script_03    — Score 7
--
-- Quarantine items requiring manual review:
--   [6]  dim_agent_enhanced   — CURSOR→CTE: verify hierarchy depth ≤ 3
--   [8]  dim_customer_enhanced— Transaction semantics: verify no partial-commit dependency
--   [13] fact_claims_enhanced — INSERT...EXEC: port sp_validate_claims DQ logic into CTE
-- =============================================================================
