-- =============================================================================
-- ETL SCRIPT 02 (ENHANCED): Load Dimension Tables
-- Description : SCD Type 1 (Product, Territory, Underwriter, ClaimType),
--               SCD Type 2 (Customer, Agent), Date dimension via WHILE loop,
--               Hierarchical agent rollup via CURSOR.
-- Complexity  : COMPLEX — CURSOR, @@ERROR, @@ROWCOUNT, RAISERROR, BEGIN TRAN
-- =============================================================================

-- =============================================================================
-- SECTION 1: Date Dimension (WHILE loop — migrate to Snowflake GENERATOR())
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_date' AND type = 'U') DROP TABLE dim_date
GO

CREATE TABLE dim_date (
    date_key            INT             NOT NULL,   -- YYYYMMDD integer
    calendar_date       DATE            NOT NULL,
    year_number         SMALLINT        NOT NULL,
    quarter_number      TINYINT         NOT NULL,
    quarter_name        CHAR(6)         NOT NULL,
    month_number        TINYINT         NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    month_abbrev        CHAR(3)         NOT NULL,
    week_of_year        TINYINT         NOT NULL,
    day_of_week         TINYINT         NOT NULL,
    day_name            VARCHAR(20)     NOT NULL,
    day_of_month        TINYINT         NOT NULL,
    day_of_year         SMALLINT        NOT NULL,
    fiscal_year         SMALLINT        NOT NULL,
    fiscal_quarter      TINYINT         NOT NULL,
    fiscal_month        TINYINT         NOT NULL,
    is_weekend          CHAR(1)         NOT NULL,
    is_holiday          CHAR(1)         NOT NULL,
    is_last_day_month   CHAR(1)         NOT NULL,
    is_last_day_quarter CHAR(1)         NOT NULL,
    season_code         VARCHAR(10)     NOT NULL,
    PRIMARY KEY (date_key)
) IN DBASPACE
GO

DECLARE @curr_date  DATE
DECLARE @end_date   DATE
DECLARE @month_num  TINYINT
DECLARE @dow        TINYINT
DECLARE @fiscal_yr  SMALLINT
DECLARE @fiscal_mo  TINYINT

SELECT @curr_date = '2000-01-01'
SELECT @end_date  = '2035-12-31'

WHILE @curr_date <= @end_date
BEGIN
    SELECT @month_num = DATEPART(month, @curr_date)
    SELECT @dow       = DATEPART(weekday, @curr_date)  -- 1=Sun in Sybase

    -- Fiscal year: company runs Apr-Mar (offset by 3 months)
    SELECT @fiscal_yr = CASE
        WHEN @month_num >= 4 THEN DATEPART(year, @curr_date)
        ELSE DATEPART(year, @curr_date) - 1
    END

    SELECT @fiscal_mo = CASE
        WHEN @month_num >= 4 THEN @month_num - 3
        ELSE @month_num + 9
    END

    INSERT INTO dim_date (
        date_key, calendar_date,
        year_number, quarter_number, quarter_name,
        month_number, month_name, month_abbrev,
        week_of_year, day_of_week, day_name,
        day_of_month, day_of_year,
        fiscal_year, fiscal_quarter, fiscal_month,
        is_weekend, is_holiday, is_last_day_month, is_last_day_quarter,
        season_code
    )
    VALUES (
        CONVERT(INT, CONVERT(VARCHAR(8), @curr_date, 112)),  -- YYYYMMDD key
        @curr_date,
        DATEPART(year,    @curr_date),
        DATEPART(quarter, @curr_date),
        'Q' + CONVERT(CHAR(1), DATEPART(quarter, @curr_date))
            + '-' + CONVERT(CHAR(4), DATEPART(year, @curr_date)),
        @month_num,
        DATENAME(month,   @curr_date),
        LEFT(DATENAME(month, @curr_date), 3),
        DATEPART(week,    @curr_date),
        @dow,
        DATENAME(weekday, @curr_date),
        DATEPART(day,     @curr_date),
        DATEPART(dayofyear, @curr_date),
        @fiscal_yr,
        CASE
            WHEN @fiscal_mo BETWEEN 1 AND 3  THEN 1
            WHEN @fiscal_mo BETWEEN 4 AND 6  THEN 2
            WHEN @fiscal_mo BETWEEN 7 AND 9  THEN 3
            ELSE 4
        END,
        @fiscal_mo,
        CASE WHEN @dow IN (1, 7) THEN 'Y' ELSE 'N' END,  -- 1=Sun, 7=Sat
        'N',  -- Populated separately by sp_mark_holidays
        CASE WHEN @curr_date = DATEADD(day, -1, DATEADD(month, 1,
             CONVERT(DATE, STR(DATEPART(year, @curr_date), 4) + '-'
             + RIGHT('0' + STR(@month_num, 2), 2) + '-01')))
             THEN 'Y' ELSE 'N' END,
        'N',  -- Last day of quarter: computed separately
        CASE @month_num
            WHEN 12 THEN 'WINTER' WHEN  1 THEN 'WINTER' WHEN  2 THEN 'WINTER'
            WHEN  3 THEN 'SPRING' WHEN  4 THEN 'SPRING' WHEN  5 THEN 'SPRING'
            WHEN  6 THEN 'SUMMER' WHEN  7 THEN 'SUMMER' WHEN  8 THEN 'SUMMER'
            ELSE 'FALL'
        END
    )

    SELECT @curr_date = DATEADD(day, 1, @curr_date)
END
GO

PRINT 'dim_date loaded: ' + CONVERT(VARCHAR, @@ROWCOUNT) + ' rows'
GO

-- Mark last day of quarter
UPDATE dim_date
SET is_last_day_quarter = 'Y'
WHERE (month_number IN (3, 6, 9, 12) AND is_last_day_month = 'Y')
GO

-- =============================================================================
-- SECTION 2: Product Dimension — SCD Type 1 (no history)
-- Pattern: UPDATE existing + INSERT new. No MERGE in Sybase.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_product' AND type = 'U')
BEGIN
    CREATE TABLE dim_product (
        product_key         INT             IDENTITY NOT NULL,
        product_code        VARCHAR(20)     NOT NULL,
        product_name        VARCHAR(200)    NOT NULL,
        product_line        VARCHAR(50)     NULL,
        lob_code            VARCHAR(10)     NULL,
        lob_description     VARCHAR(100)    NULL,
        coverage_type       VARCHAR(50)     NULL,
        base_rate           DECIMAL(10,6)   NULL,
        min_premium         MONEY           NULL,
        max_coverage        MONEY           NULL,
        is_commercial       CHAR(1)         NULL,
        is_reinsurable      CHAR(1)         NULL,
        filing_state        CHAR(2)         NULL,
        effective_date      DATE            NULL,
        expiry_date         DATE            NULL,
        dw_created_ts       DATETIME        NOT NULL DEFAULT GETDATE(),
        dw_updated_ts       DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (product_key)
    ) IN DBASPACE
END
GO

-- SCD1 UPDATE (change in place — overwrite all attributes)
UPDATE dim_product
SET
    product_name        = s.product_name,
    product_line        = s.product_line,
    lob_code            = s.lob_code,
    lob_description     = s.lob_description,
    coverage_type       = s.coverage_type,
    base_rate           = s.base_rate,
    min_premium         = s.min_premium,
    max_coverage        = s.max_coverage,
    is_commercial       = s.is_commercial,
    is_reinsurable      = s.is_reinsurable,
    filing_state        = s.filing_state,
    effective_date      = s.effective_date,
    expiry_date         = s.expiry_date,
    dw_updated_ts       = GETDATE()
FROM dim_product d
INNER JOIN stg_products s ON s.product_code = d.product_code
GO

PRINT STR(@@ROWCOUNT, 6) + ' product rows updated (SCD1)'   -- STR() function
GO

-- SCD1 INSERT new products
INSERT INTO dim_product (
    product_code, product_name, product_line, lob_code, lob_description,
    coverage_type, base_rate, min_premium, max_coverage,
    is_commercial, is_reinsurable, filing_state, effective_date, expiry_date
)
SELECT
    s.product_code, s.product_name, s.product_line, s.lob_code, s.lob_description,
    s.coverage_type, s.base_rate, s.min_premium, s.max_coverage,
    s.is_commercial, s.is_reinsurable, s.filing_state, s.effective_date, s.expiry_date
FROM stg_products s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_product d WHERE d.product_code = s.product_code
)
GO

PRINT STR(@@ROWCOUNT, 6) + ' product rows inserted (SCD1)'
GO

-- =============================================================================
-- SECTION 3: Agent Dimension — SCD Type 2 with hierarchical CURSOR
-- Pattern: CURSOR to process agents level-by-level (bottom-up hierarchy)
-- Migration note: CURSOR → set-based CTE hierarchy or recursive query in Snowflake
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_agent' AND type = 'U')
BEGIN
    CREATE TABLE dim_agent (
        agent_key           INT             IDENTITY NOT NULL,
        agent_id            INT             NOT NULL,
        agent_number        VARCHAR(20)     NOT NULL,
        agent_name          VARCHAR(200)    NOT NULL,
        agency_id           INT             NULL,
        agency_name         VARCHAR(200)    NULL,
        parent_agency_id    INT             NULL,
        hierarchy_level     TINYINT         NULL,
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
        effective_date      DATE            NOT NULL DEFAULT GETDATE(),
        expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
        is_current          CHAR(1)         NOT NULL DEFAULT 'Y',
        dw_created_ts       DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (agent_key)
    ) IN DBASPACE
END
GO

-- Step 1: Expire changed records (SCD2 expiry)
UPDATE dim_agent
SET
    expiry_date   = DATEADD(day, -1, GETDATE()),
    is_current    = 'N'
FROM dim_agent d
INNER JOIN stg_agents s ON s.agent_id = d.agent_id AND d.is_current = 'Y'
WHERE
    d.agent_name        <> s.agent_name
    OR ISNULL(d.agency_id, -1) <> ISNULL(s.agency_id, -1)
    OR ISNULL(d.region_code, '') <> ISNULL(s.region_code, '')
    OR ISNULL(d.channel_code,'') <> ISNULL(s.channel_code,'')
    OR ISNULL(d.commission_tier,'') <> ISNULL(s.commission_tier,'')
    OR d.active_flag    <> s.active_flag
GO

DECLARE @expired_agents INT
SELECT  @expired_agents = @@ROWCOUNT
PRINT 'SCD2 agents expired: ' + CONVERT(VARCHAR, @expired_agents)
GO

-- Step 2: CURSOR — resolve hierarchy path bottom-up before inserting new rows
-- This is the key CURSOR pattern: each agent needs its full path materialized.
-- CURSOR processes leaf agents first, then walks up the tree.

DECLARE @v_agent_id         INT
DECLARE @v_agency_id        INT
DECLARE @v_parent_agency_id INT
DECLARE @v_level            TINYINT
DECLARE @v_path             VARCHAR(500)
DECLARE @v_region_desc      VARCHAR(100)
DECLARE @v_channel_desc     VARCHAR(100)
DECLARE @v_rows_inserted    INT

-- Temp table to hold resolved hierarchy for new/changed agents
CREATE TABLE #agent_hierarchy (
    agent_id        INT         NOT NULL,
    hierarchy_path  VARCHAR(500) NULL,
    region_desc     VARCHAR(100) NULL,
    channel_desc    VARCHAR(100) NULL
)

-- CURSOR: iterate agents that need new SCD2 rows
DECLARE agent_cursor CURSOR FOR
    SELECT
        s.agent_id,
        s.agency_id,
        s.parent_agency_id,
        s.hierarchy_level,
        s.region_code,
        s.channel_code
    FROM stg_agents s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_agent d
        WHERE d.agent_id = s.agent_id AND d.is_current = 'Y'
    )
    ORDER BY s.hierarchy_level ASC, s.agent_id ASC
FOR READ ONLY

OPEN agent_cursor

FETCH NEXT FROM agent_cursor
    INTO @v_agent_id, @v_agency_id, @v_parent_agency_id, @v_level,
         @v_region_desc, @v_channel_desc

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Build materialized path: ROOT > PARENT_AGENCY > AGENCY > AGENT
    SELECT @v_path = CASE
        WHEN @v_parent_agency_id IS NULL AND @v_agency_id IS NULL
            THEN CONVERT(VARCHAR, @v_agent_id)
        WHEN @v_parent_agency_id IS NULL
            THEN CONVERT(VARCHAR, @v_agency_id)
                 + '>' + CONVERT(VARCHAR, @v_agent_id)
        ELSE
            CONVERT(VARCHAR, @v_parent_agency_id)
            + '>' + CONVERT(VARCHAR, @v_agency_id)
            + '>' + CONVERT(VARCHAR, @v_agent_id)
    END

    -- Lookup human-readable region / channel descriptions
    SELECT @v_region_desc = CASE @v_region_desc
        WHEN 'NE'  THEN 'Northeast'
        WHEN 'SE'  THEN 'Southeast'
        WHEN 'MW'  THEN 'Midwest'
        WHEN 'SW'  THEN 'Southwest'
        WHEN 'W'   THEN 'West'
        WHEN 'NW'  THEN 'Northwest'
        ELSE ISNULL(@v_region_desc, 'Unknown Region')
    END

    SELECT @v_channel_desc = CASE @v_channel_desc
        WHEN 'INDEPENDENT' THEN 'Independent Agent'
        WHEN 'CAPTIVE'     THEN 'Captive Agent'
        WHEN 'DIRECT'      THEN 'Direct to Consumer'
        WHEN 'BROKER'      THEN 'Wholesale Broker'
        WHEN 'MGA'         THEN 'Managing General Agent'
        ELSE ISNULL(@v_channel_desc, 'Other')
    END

    INSERT INTO #agent_hierarchy (agent_id, hierarchy_path, region_desc, channel_desc)
    VALUES (@v_agent_id, @v_path, @v_region_desc, @v_channel_desc)

    FETCH NEXT FROM agent_cursor
        INTO @v_agent_id, @v_agency_id, @v_parent_agency_id, @v_level,
             @v_region_desc, @v_channel_desc
END

CLOSE     agent_cursor
DEALLOCATE agent_cursor
GO

-- Insert new / changed SCD2 agent rows using resolved hierarchy
INSERT INTO dim_agent (
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
    ISNULL(h.hierarchy_path, CONVERT(VARCHAR, s.agent_id)),
    s.license_state, s.license_number, s.license_expiry,
    s.region_code,
    ISNULL(h.region_desc, s.region_code),
    s.district_code,
    s.channel_code,
    ISNULL(h.channel_desc, s.channel_code),
    s.commission_tier,
    s.active_flag, s.hire_date, s.termination_date,
    GETDATE(), '9999-12-31', 'Y'
FROM stg_agents s
LEFT JOIN #agent_hierarchy h ON h.agent_id = s.agent_id
WHERE NOT EXISTS (
    SELECT 1 FROM dim_agent d
    WHERE d.agent_id = s.agent_id AND d.is_current = 'Y'
)
GO

SELECT @v_rows_inserted = @@ROWCOUNT
PRINT 'SCD2 agent rows inserted: ' + CONVERT(VARCHAR, @v_rows_inserted)

IF @@ERROR <> 0
BEGIN
    RAISERROR ('ERROR: dim_agent SCD2 insert failed. Check ETL log for details.', 16, 1)
    GOTO cleanup_agent
END

cleanup_agent:
DROP TABLE #agent_hierarchy
GO

-- =============================================================================
-- SECTION 4: Customer Dimension — SCD Type 2 with temp table + @@ERROR handling
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_customer' AND type = 'U')
BEGIN
    CREATE TABLE dim_customer (
        customer_key        INT             IDENTITY NOT NULL,
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
        lifetime_value      MONEY           NULL,
        churn_risk_score    DECIMAL(5,4)    NULL,
        is_vip              CHAR(1)         NULL,
        effective_date      DATE            NOT NULL DEFAULT GETDATE(),
        expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
        is_current          CHAR(1)         NOT NULL DEFAULT 'Y',
        dw_created_ts       DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (customer_key)
    ) IN DBASPACE
END
GO

-- Step 1: Stage changed customers into temp table
CREATE TABLE #changed_customers (
    customer_id BIGINT NOT NULL
)

INSERT INTO #changed_customers (customer_id)
SELECT s.customer_id
FROM stg_customers s
INNER JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current = 'Y'
WHERE
    ISNULL(d.state_code,'')     <> ISNULL(s.state_code,'')
    OR ISNULL(d.credit_score,-1) <> ISNULL(s.credit_score,-1)
    OR ISNULL(d.fico_band,'')   <> ISNULL(s.fico_band,'')
    OR ISNULL(d.customer_segment,'') <> ISNULL(s.customer_segment,'')
    OR ISNULL(d.address_line1,'') <> ISNULL(s.address_line1,'')
    OR ISNULL(d.email,'')       <> ISNULL(s.email,'')
    OR ISNULL(d.is_vip,'N')     <> ISNULL(s.is_vip,'N')
GO

DECLARE @changed_count INT
SELECT  @changed_count = COUNT(*) FROM #changed_customers
PRINT 'Changed customers identified: ' + CONVERT(VARCHAR, @changed_count)
GO

-- Step 2: Expire changed customers
BEGIN TRAN expire_customers

    UPDATE dim_customer
    SET expiry_date = DATEADD(day, -1, GETDATE()),
        is_current  = 'N'
    FROM dim_customer d
    INNER JOIN #changed_customers c ON c.customer_id = d.customer_id
    WHERE d.is_current = 'Y'

    IF @@ERROR <> 0
    BEGIN
        ROLLBACK TRAN expire_customers
        RAISERROR ('FATAL: Customer SCD2 expiry failed. Transaction rolled back.', 16, 1)
        DROP TABLE #changed_customers
        RETURN
    END

COMMIT TRAN expire_customers
GO

-- Step 3: Insert new SCD2 rows for changed + genuinely new customers
BEGIN TRAN insert_customers

    INSERT INTO dim_customer (
        customer_id, full_name, date_of_birth, age_band,
        gender_code, gender_description, marital_status, marital_description,
        address_line1, city, state_code, state_name, region, zip_code, email,
        credit_score, credit_band, fico_band, customer_segment,
        acquisition_source, lifetime_value, churn_risk_score, is_vip,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.customer_id,
        LTRIM(RTRIM(ISNULL(s.first_name,'') + ' ' + ISNULL(s.last_name,''))),
        s.date_of_birth,
        -- Age band computed at load time (no window functions in Sybase IQ)
        CASE
            WHEN DATEDIFF(year, s.date_of_birth, GETDATE()) < 25 THEN '18-24'
            WHEN DATEDIFF(year, s.date_of_birth, GETDATE()) < 35 THEN '25-34'
            WHEN DATEDIFF(year, s.date_of_birth, GETDATE()) < 45 THEN '35-44'
            WHEN DATEDIFF(year, s.date_of_birth, GETDATE()) < 55 THEN '45-54'
            WHEN DATEDIFF(year, s.date_of_birth, GETDATE()) < 65 THEN '55-64'
            WHEN s.date_of_birth IS NOT NULL                      THEN '65+'
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
        -- State name lookup via CASE (full 50-state table in production)
        CASE s.state_code
            WHEN 'AL' THEN 'Alabama'    WHEN 'AK' THEN 'Alaska'
            WHEN 'AZ' THEN 'Arizona'    WHEN 'AR' THEN 'Arkansas'
            WHEN 'CA' THEN 'California' WHEN 'CO' THEN 'Colorado'
            WHEN 'CT' THEN 'Connecticut' WHEN 'FL' THEN 'Florida'
            WHEN 'GA' THEN 'Georgia'    WHEN 'IL' THEN 'Illinois'
            WHEN 'NY' THEN 'New York'   WHEN 'TX' THEN 'Texas'
            ELSE ISNULL(s.state_code, 'Unknown')
        END,
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
        GETDATE(), '9999-12-31', 'Y'
    FROM stg_customers s
    WHERE s.customer_id IN (SELECT customer_id FROM #changed_customers)
       OR NOT EXISTS (
           SELECT 1 FROM dim_customer d
           WHERE d.customer_id = s.customer_id
       )

    IF @@ERROR <> 0
    BEGIN
        ROLLBACK TRAN insert_customers
        RAISERROR ('FATAL: Customer SCD2 insert failed. Transaction rolled back.', 16, 1)
        DROP TABLE #changed_customers
        RETURN
    END

    DECLARE @cust_inserted INT
    SELECT  @cust_inserted = @@ROWCOUNT

COMMIT TRAN insert_customers

PRINT 'dim_customer SCD2 complete. Inserted: ' + CONVERT(VARCHAR, @cust_inserted)
DROP TABLE #changed_customers
GO

-- =============================================================================
-- SECTION 5: Territory Dimension — SCD Type 1 with COMPUTE BY audit
-- COMPUTE BY is Sybase-specific — migration: window functions in Snowflake
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'dim_territory' AND type = 'U')
BEGIN
    CREATE TABLE dim_territory (
        territory_key   INT             IDENTITY NOT NULL,
        territory_code  VARCHAR(10)     NOT NULL,
        territory_name  VARCHAR(100)    NOT NULL,
        state_code      CHAR(2)         NOT NULL,
        state_name      VARCHAR(100)    NULL,
        region_code     VARCHAR(10)     NOT NULL,
        region_name     VARCHAR(100)    NULL,
        zone_code       VARCHAR(10)     NULL,
        zone_name       VARCHAR(100)    NULL,
        catastrophe_zone VARCHAR(10)    NULL,
        wind_pool_flag  CHAR(1)         NULL,
        territory_count_in_region INT  NULL,
        active_flag     CHAR(1)         NULL,
        dw_created_ts   DATETIME        NOT NULL DEFAULT GETDATE(),
        dw_updated_ts   DATETIME        NOT NULL DEFAULT GETDATE(),
        PRIMARY KEY (territory_key)
    ) IN DBASPACE
END
GO

-- SCD1 UPDATE
UPDATE dim_territory
SET
    territory_name          = s.territory_name,
    state_code              = s.state_code,
    region_code             = s.region_code,
    region_name             = s.region_name,
    zone_code               = s.zone_code,
    zone_name               = s.zone_name,
    catastrophe_zone        = s.catastrophe_zone,
    wind_pool_flag          = s.wind_pool_flag,
    active_flag             = s.active_flag,
    dw_updated_ts           = GETDATE()
FROM dim_territory d
INNER JOIN stg_territories s ON s.territory_code = d.territory_code
GO

-- SCD1 INSERT
INSERT INTO dim_territory (
    territory_code, territory_name, state_code, region_code, region_name,
    zone_code, zone_name, catastrophe_zone, wind_pool_flag, active_flag
)
SELECT s.territory_code, s.territory_name, s.state_code, s.region_code, s.region_name,
       s.zone_code, s.zone_name, s.catastrophe_zone, s.wind_pool_flag, s.active_flag
FROM stg_territories s
WHERE NOT EXISTS (SELECT 1 FROM dim_territory d WHERE d.territory_code = s.territory_code)
GO

-- COMPUTE BY: audit count of territories per region
-- Migration: SELECT region_code, COUNT(*), SUM() ... GROUP BY in Snowflake
-- Or window function: COUNT(*) OVER (PARTITION BY region_code)
SELECT region_code, territory_code, territory_name, wind_pool_flag
FROM dim_territory
WHERE active_flag = 'Y'
ORDER BY region_code, territory_code
COMPUTE COUNT(territory_code), SUM(CASE WHEN wind_pool_flag='Y' THEN 1 ELSE 0 END)
    BY region_code
GO

-- Update territory count denormalization
UPDATE dim_territory
SET territory_count_in_region = rc.cnt
FROM dim_territory d
INNER JOIN (
    SELECT region_code, COUNT(*) AS cnt
    FROM dim_territory
    WHERE active_flag = 'Y'
    GROUP BY region_code
) rc ON rc.region_code = d.region_code
GO

PRINT 'Script 02 complete: Dimensions loaded'
GO
