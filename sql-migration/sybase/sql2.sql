-- ============================================================================= 

-- ETL SCRIPT 02: Load Dimension Tables (SCD Type 2) 

-- Description : Transform staged data and load into star schema dimensions. 

--               Implements SCD Type 2 for Customer and Agent dims, 

--               SCD Type 1 for Product and Date dims. 

-- Target DB   : DataWarehouse (Sybase IQ) 

-- Author      : DW Team 

-- Created     : 2009-04-01 

-- Modified    : 2013-06-18 

-- ============================================================================= 

  

-- ============================================================ 

-- SECTION 1: DIM_DATE (static reference, load once per year) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_date' AND type = 'U') 

    DROP TABLE dim_date 

GO 

  

CREATE TABLE dim_date ( 

    date_key            INT             NOT NULL PRIMARY KEY,  -- YYYYMMDD 

    calendar_date       DATE            NOT NULL, 

    day_of_week         TINYINT         NOT NULL,   -- 1=Sun, 7=Sat 

    day_name            VARCHAR(10)     NOT NULL, 

    day_of_month        TINYINT         NOT NULL, 

    day_of_year         SMALLINT        NOT NULL, 

    week_of_year        TINYINT         NOT NULL, 

    month_number        TINYINT         NOT NULL, 

    month_name          VARCHAR(10)     NOT NULL, 

    month_short         CHAR(3)         NOT NULL, 

    quarter_number      TINYINT         NOT NULL, 

    quarter_name        CHAR(2)         NOT NULL, 

    year_number         SMALLINT        NOT NULL, 

    fiscal_month        TINYINT         NULL,       -- if fiscal year differs 

    fiscal_quarter      TINYINT         NULL, 

    fiscal_year         SMALLINT        NULL, 

    is_weekday          CHAR(1)         NOT NULL DEFAULT 'Y', 

    is_holiday          CHAR(1)         NOT NULL DEFAULT 'N', 

    holiday_name        VARCHAR(50)     NULL, 

    is_month_end        CHAR(1)         NOT NULL DEFAULT 'N', 

    is_quarter_end      CHAR(1)         NOT NULL DEFAULT 'N', 

    is_year_end         CHAR(1)         NOT NULL DEFAULT 'N' 

) 

GO 

  

-- Populate dim_date for 2000-01-01 through 2030-12-31 

-- Sybase IQ approach: use a numbers helper table or loop 

DECLARE @start_date  DATE 

DECLARE @end_date    DATE 

DECLARE @curr_date   DATE 

  

SELECT @start_date = '2000-01-01' 

SELECT @end_date   = '2030-12-31' 

SELECT @curr_date  = @start_date 

  

WHILE @curr_date <= @end_date 

BEGIN 

    INSERT INTO dim_date ( 

        date_key, calendar_date, 

        day_of_week, day_name, day_of_month, day_of_year, 

        week_of_year, month_number, month_name, month_short, 

        quarter_number, quarter_name, year_number, 

        fiscal_month, fiscal_quarter, fiscal_year, 

        is_weekday, is_month_end, is_quarter_end, is_year_end 

    ) 

    VALUES ( 

        CONVERT(INT, CONVERT(CHAR(8), @curr_date, 112)), 

        @curr_date, 

        DATEPART(WEEKDAY, @curr_date), 

        DATENAME(WEEKDAY, @curr_date), 

        DATEPART(DAY,     @curr_date), 

        DATEPART(DAYOFYEAR, @curr_date), 

        DATEPART(WEEK,    @curr_date), 

        DATEPART(MONTH,   @curr_date), 

        DATENAME(MONTH,   @curr_date), 

        SUBSTRING(DATENAME(MONTH, @curr_date), 1, 3), 

        DATEPART(QUARTER, @curr_date), 

        'Q' + CONVERT(CHAR(1), DATEPART(QUARTER, @curr_date)), 

        DATEPART(YEAR,    @curr_date), 

        -- Fiscal year offset by 3 months (April start) 

        CASE WHEN DATEPART(MONTH, @curr_date) >= 4 

             THEN DATEPART(MONTH, @curr_date) - 3 

             ELSE DATEPART(MONTH, @curr_date) + 9 END, 

        CASE WHEN DATEPART(MONTH, @curr_date) BETWEEN 4 AND 6   THEN 1 

             WHEN DATEPART(MONTH, @curr_date) BETWEEN 7 AND 9   THEN 2 

             WHEN DATEPART(MONTH, @curr_date) BETWEEN 10 AND 12 THEN 3 

             ELSE 4 END, 

        CASE WHEN DATEPART(MONTH, @curr_date) >= 4 

             THEN DATEPART(YEAR, @curr_date) 

             ELSE DATEPART(YEAR, @curr_date) - 1 END, 

        CASE WHEN DATEPART(WEEKDAY, @curr_date) IN (1,7) THEN 'N' ELSE 'Y' END, 

        CASE WHEN @curr_date = DATEADD(DAY, -1, DATEADD(MONTH, 1, 

             CONVERT(DATE, CONVERT(CHAR(6), @curr_date, 112) + '01'))) 

             THEN 'Y' ELSE 'N' END, 

        'N',   -- quarter end: updated separately 

        CASE WHEN DATEPART(MONTH, @curr_date) = 12 

              AND DATEPART(DAY,   @curr_date) = 31 THEN 'Y' ELSE 'N' END 

    ) 

  

    SELECT @curr_date = DATEADD(DAY, 1, @curr_date) 

END 

GO 

  

-- Mark quarter-end dates 

UPDATE dim_date 

SET    is_quarter_end = 'Y' 

WHERE  is_month_end   = 'Y' 

AND    month_number IN (3, 6, 9, 12) 

GO 

  

-- ============================================================ 

-- SECTION 2: DIM_PRODUCT  (SCD Type 1 - overwrite) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_product' AND type = 'U') 

    DROP TABLE dim_product 

GO 

  

CREATE TABLE dim_product ( 

    product_key         INT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

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

    dw_insert_ts        DATETIME          NOT NULL DEFAULT GETDATE(), 

    dw_update_ts        DATETIME          NOT NULL DEFAULT GETDATE() 

) 

GO 

  

-- SCD1 Merge for dim_product (update-or-insert) 

-- Step A: Update changed records 

UPDATE dp 

SET    dp.product_name      = sp.product_name, 

       dp.product_line      = ISNULL(sp.product_line, 'UNKNOWN'), 

       dp.lob_code          = sp.lob_code, 

       dp.lob_description   = CASE sp.lob_code 

                                  WHEN 'AUTO' THEN 'Personal Auto' 

                                  WHEN 'HOME' THEN 'Homeowners' 

                                  WHEN 'COMM' THEN 'Commercial Lines' 

                                  WHEN 'LIFE' THEN 'Life and Annuity' 

                                  ELSE 'Other' 

                              END, 

       dp.sub_lob_code      = sp.sub_lob_code, 

       dp.coverage_type     = sp.coverage_type, 

       dp.rating_plan       = sp.rating_plan, 

       dp.min_premium       = sp.min_premium, 

       dp.max_coverage      = sp.max_coverage, 

       dp.is_active         = sp.active_flag, 

       dp.effective_date    = sp.effective_date, 

       dp.discontinue_date  = sp.discontinue_date, 

       dp.dw_update_ts      = GETDATE() 

FROM   dim_product dp 

INNER JOIN stg_products sp ON dp.product_code = sp.product_code 

WHERE  dp.product_name      <> sp.product_name 

    OR dp.lob_code          <> ISNULL(sp.lob_code, dp.lob_code) 

    OR dp.is_active         <> sp.active_flag 

GO 

  

-- Step B: Insert new products 

INSERT INTO dim_product ( 

    product_code, product_name, product_line, 

    lob_code, lob_description, sub_lob_code, 

    coverage_type, rating_plan, min_premium, max_coverage, 

    is_active, effective_date, discontinue_date 

) 

SELECT 

    sp.product_code, 

    sp.product_name, 

    ISNULL(sp.product_line, 'UNKNOWN'), 

    sp.lob_code, 

    CASE sp.lob_code 

        WHEN 'AUTO' THEN 'Personal Auto' 

        WHEN 'HOME' THEN 'Homeowners' 

        WHEN 'COMM' THEN 'Commercial Lines' 

        WHEN 'LIFE' THEN 'Life and Annuity' 

        ELSE 'Other' 

    END, 

    sp.sub_lob_code, 

    sp.coverage_type, 

    sp.rating_plan, 

    sp.min_premium, 

    sp.max_coverage, 

    sp.active_flag, 

    sp.effective_date, 

    sp.discontinue_date 

FROM stg_products sp 

WHERE NOT EXISTS ( 

    SELECT 1 FROM dim_product dp WHERE dp.product_code = sp.product_code 

) 

GO 

  

-- ============================================================ 

-- SECTION 3: DIM_AGENT  (SCD Type 2 - preserve history) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_agent' AND type = 'U') 

    DROP TABLE dim_agent 

GO 

  

CREATE TABLE dim_agent ( 

    agent_key           INT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    agent_id            INT               NOT NULL,   -- natural key 

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

    dw_insert_ts        DATETIME          NOT NULL DEFAULT GETDATE(), 

    dw_update_ts        DATETIME          NOT NULL DEFAULT GETDATE() 

) 

GO 

  

-- SCD2 logic: expire old records and insert new versions 

  

-- Step A: Identify changed agents and expire current record 

UPDATE da 

SET    da.scd_end_date      = DATEADD(DAY, -1, GETDATE()), 

       da.scd_current_flag  = 'N', 

       da.dw_update_ts      = GETDATE() 

FROM   dim_agent da 

INNER JOIN stg_agents sa ON da.agent_id = sa.agent_id 

WHERE  da.scd_current_flag = 'Y' 

AND ( 

       da.agency_id         <> ISNULL(sa.agency_id, -1) 

    OR da.region_code       <> ISNULL(sa.region_code, 'UNKN') 

    OR da.channel_code      <> ISNULL(sa.channel_code, 'UNKN') 

    OR da.is_active         <> sa.active_flag 

    OR da.license_expiry    <> ISNULL(sa.license_expiry, da.license_expiry) 

) 

GO 

  

-- Step B: Insert new version for changed agents + insert brand new agents 

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

    CAST(GETDATE() AS DATE), 

    NULL,   -- current record 

    'Y', 

    ISNULL(( 

        SELECT MAX(scd_version) + 1 

        FROM dim_agent x WHERE x.agent_id = sa.agent_id 

    ), 1) 

FROM stg_agents sa 

WHERE NOT EXISTS ( 

    SELECT 1 FROM dim_agent da 

    WHERE  da.agent_id          = sa.agent_id 

    AND    da.scd_current_flag  = 'Y' 

) 

GO 

  

-- ============================================================ 

-- SECTION 4: DIM_CUSTOMER  (SCD Type 2 - preserve history) 

-- ============================================================ 

  

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_customer' AND type = 'U') 

    DROP TABLE dim_customer 

GO 

  

CREATE TABLE dim_customer ( 

    customer_key        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, 

    customer_id         BIGINT               NOT NULL,   -- natural key 

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

    dw_insert_ts        DATETIME             NOT NULL DEFAULT GETDATE(), 

    dw_update_ts        DATETIME             NOT NULL DEFAULT GETDATE() 

) 

GO 

  

-- Expire records that have changed on tracked attributes 

UPDATE dc 

SET    dc.scd_end_date     = DATEADD(DAY, -1, GETDATE()), 

       dc.scd_current_flag = 'N', 

       dc.dw_update_ts     = GETDATE() 

FROM   dim_customer dc 

INNER JOIN stg_customers sc ON dc.customer_id = sc.customer_id 

WHERE  dc.scd_current_flag = 'Y' 

AND ( 

       ISNULL(dc.state_code,    'XX') <> ISNULL(sc.state_code,    'XX') 

    OR ISNULL(dc.zip_code,      '00000') <> ISNULL(sc.zip_code,   '00000') 

    OR ISNULL(dc.marital_status,'X')  <> ISNULL(sc.marital_status,'X') 

    OR ISNULL(dc.credit_score,  0)    <> ISNULL(sc.credit_score,   0) 

    OR ISNULL(dc.customer_segment,'') <> ISNULL(sc.customer_segment,'') 

) 

GO 

  

-- Insert new / new-version customer records 

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

    LTRIM(RTRIM(ISNULL(sc.first_name,'') + ' ' + ISNULL(sc.last_name,''))), 

    sc.first_name, 

    sc.last_name, 

    sc.date_of_birth, 

    -- Derive age band from DOB 

    CASE 

        WHEN DATEDIFF(YEAR, sc.date_of_birth, GETDATE()) < 26 THEN '18-25' 

        WHEN DATEDIFF(YEAR, sc.date_of_birth, GETDATE()) < 36 THEN '26-35' 

        WHEN DATEDIFF(YEAR, sc.date_of_birth, GETDATE()) < 46 THEN '36-45' 

        WHEN DATEDIFF(YEAR, sc.date_of_birth, GETDATE()) < 56 THEN '46-55' 

        WHEN DATEDIFF(YEAR, sc.date_of_birth, GETDATE()) < 66 THEN '56-65' 

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

    -- State name lookup (abbreviated for script) 

    CASE sc.state_code 

        WHEN 'CA' THEN 'California'  WHEN 'TX' THEN 'Texas' 

        WHEN 'FL' THEN 'Florida'     WHEN 'NY' THEN 'New York' 

        WHEN 'IL' THEN 'Illinois'    WHEN 'OH' THEN 'Ohio' 

        WHEN 'PA' THEN 'Pennsylvania' ELSE sc.state_code 

    END, 

    LEFT(sc.zip_code, 5), 

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

        WHEN sc.credit_score >= 800 THEN 'Exceptional (800+)' 

        WHEN sc.credit_score >= 740 THEN 'Very Good (740-799)' 

        WHEN sc.credit_score >= 670 THEN 'Good (670-739)' 

        WHEN sc.credit_score >= 580 THEN 'Fair (580-669)' 

        WHEN sc.credit_score IS NOT NULL THEN 'Poor (<580)' 

        ELSE 'Unknown' 

    END, 

    sc.customer_segment, 

    sc.acquisition_source, 

    CAST(GETDATE() AS DATE), 

    NULL, 

    'Y', 

    ISNULL(( 

        SELECT MAX(scd_version) + 1 

        FROM dim_customer x WHERE x.customer_id = sc.customer_id 

    ), 1) 

FROM stg_customers sc 

WHERE NOT EXISTS ( 

    SELECT 1 FROM dim_customer dc 

    WHERE  dc.customer_id      = sc.customer_id 

    AND    dc.scd_current_flag = 'Y' 

) 

GO 

  

PRINT 'Dimension tables loaded successfully' 

PRINT 'dim_date    : ' + CONVERT(VARCHAR, (SELECT COUNT(*) FROM dim_date))    + ' rows' 

PRINT 'dim_product : ' + CONVERT(VARCHAR, (SELECT COUNT(*) FROM dim_product)) + ' rows' 

PRINT 'dim_agent   : ' + CONVERT(VARCHAR, (SELECT COUNT(*) FROM dim_agent))   + ' rows' 

PRINT 'dim_customer: ' + CONVERT(VARCHAR, (SELECT COUNT(*) FROM dim_customer))+ ' rows' 

GO 
