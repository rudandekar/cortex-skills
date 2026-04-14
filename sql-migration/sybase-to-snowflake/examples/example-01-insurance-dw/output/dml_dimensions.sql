-- =============================================================================
-- [META] Converted by: sybase-to-snowflake skill v0.2.0
-- [META] Source file:   sql2.sql
-- [META] Source DB:     Sybase ASE/IQ (DataWarehouse)
-- [META] Target DB:     Snowflake
-- [META] Converted at:  2026-04-02
-- [META] Layer:         L2_dimension (DML)
-- [META] Objects:       dim_date, dim_product, dim_agent, dim_customer
-- =============================================================================


-- ============================================================
-- SECTION 1: Populate DIM_DATE (2000-01-01 through 2030-12-31)
-- [CONVERT] DECLARE @var / WHILE / BEGIN...END →
--           Snowflake set-based approach using GENERATOR + DATE_FROM_PARTS
-- [CONVERT] DATEPART(x, d) → DATE_PART(x, d) / EXTRACT()
-- [CONVERT] DATENAME(WEEKDAY, d) → DAYNAME(d)
-- [CONVERT] DATENAME(MONTH, d) → MONTHNAME(d)
-- [CONVERT] CONVERT(INT, CONVERT(CHAR(8), d, 112)) → TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))
-- [CONVERT] SUBSTRING(..., 1, 3) → LEFT(..., 3)
-- ============================================================

INSERT INTO dim_date (
    date_key, calendar_date,
    day_of_week, day_name, day_of_month, day_of_year,
    week_of_year, month_number, month_name, month_short,
    quarter_number, quarter_name, year_number,
    fiscal_month, fiscal_quarter, fiscal_year,
    is_weekday, is_month_end, is_quarter_end, is_year_end
)
SELECT
    TO_NUMBER(TO_CHAR(d.calendar_date, 'YYYYMMDD'))     AS date_key,
    d.calendar_date,
    DATE_PART(DOW, d.calendar_date) + 1                  AS day_of_week,        -- [CONVERT] Snowflake DOW is 0-based (0=Sun with WEEK_START=0 default); +1 → 1=Sun..7=Sat matching Sybase
    DAYNAME(d.calendar_date)                              AS day_name,           -- [CONVERT] DATENAME(WEEKDAY, ...)
    DATE_PART(DAY, d.calendar_date)                       AS day_of_month,
    DATE_PART(DOY, d.calendar_date)                       AS day_of_year,        -- [CONVERT] DAYOFYEAR → DOY
    DATE_PART(WEEK, d.calendar_date)                      AS week_of_year,
    DATE_PART(MONTH, d.calendar_date)                     AS month_number,
    MONTHNAME(d.calendar_date)                            AS month_name,         -- [CONVERT] DATENAME(MONTH, ...)
    LEFT(MONTHNAME(d.calendar_date), 3)                   AS month_short,        -- [CONVERT] SUBSTRING(..., 1, 3) → LEFT()
    DATE_PART(QUARTER, d.calendar_date)                   AS quarter_number,
    'Q' || DATE_PART(QUARTER, d.calendar_date)::VARCHAR   AS quarter_name,       -- [CONVERT] 'Q' + CONVERT(...) → || cast
    DATE_PART(YEAR, d.calendar_date)                      AS year_number,
    -- Fiscal year offset by 3 months (April start)
    CASE WHEN DATE_PART(MONTH, d.calendar_date) >= 4
         THEN DATE_PART(MONTH, d.calendar_date) - 3
         ELSE DATE_PART(MONTH, d.calendar_date) + 9 END  AS fiscal_month,
    CASE WHEN DATE_PART(MONTH, d.calendar_date) BETWEEN 4 AND 6   THEN 1
         WHEN DATE_PART(MONTH, d.calendar_date) BETWEEN 7 AND 9   THEN 2
         WHEN DATE_PART(MONTH, d.calendar_date) BETWEEN 10 AND 12 THEN 3
         ELSE 4 END                                       AS fiscal_quarter,
    CASE WHEN DATE_PART(MONTH, d.calendar_date) >= 4
         THEN DATE_PART(YEAR, d.calendar_date)
         ELSE DATE_PART(YEAR, d.calendar_date) - 1 END   AS fiscal_year,
    CASE WHEN DATE_PART(DOW, d.calendar_date) IN (0, 6)
         THEN 'N' ELSE 'Y' END                           AS is_weekday,         -- [CONVERT] Snowflake DOW (WEEK_START=0): 0=Sun, 5=Fri, 6=Sat; weekends = 0,6 (Sun,Sat)
    CASE WHEN d.calendar_date = LAST_DAY(d.calendar_date)
         THEN 'Y' ELSE 'N' END                           AS is_month_end,       -- [CONVERT] complex DATEADD expression → LAST_DAY()
    'N'                                                   AS is_quarter_end,     -- updated below
    CASE WHEN DATE_PART(MONTH, d.calendar_date) = 12
          AND DATE_PART(DAY, d.calendar_date)   = 31
         THEN 'Y' ELSE 'N' END                           AS is_year_end
FROM (
    -- [CONVERT] WHILE loop → set-based GENERATOR approach
    -- Generates 11,323 rows for 2000-01-01 through 2030-12-31
    SELECT DATEADD(DAY, seq4.seq, '2000-01-01'::DATE) AS calendar_date
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 11323))    -- 31 years of days
    ) seq4
    WHERE DATEADD(DAY, seq4.seq, '2000-01-01'::DATE) <= '2030-12-31'::DATE
) d;

-- [CONVERT] Note on day_of_week alignment — VERIFIED:
-- Sybase DATEPART(WEEKDAY, ...) returns 1=Sunday through 7=Saturday.
-- Snowflake DATE_PART(DOW, ...) with default WEEK_START=0 returns 0=Sunday through 6=Saturday.
-- The +1 offset above produces 1=Sunday..7=Saturday, matching Sybase exactly.
-- The is_weekday check uses IN (0, 6) which correctly identifies Sunday and Saturday.
-- If WEEK_START is changed at session/account level, both day_of_week and is_weekday will break.

-- Mark quarter-end dates
UPDATE dim_date
SET    is_quarter_end = 'Y'
WHERE  is_month_end   = 'Y'
AND    month_number IN (3, 6, 9, 12);


-- ============================================================
-- SECTION 2: DIM_PRODUCT SCD Type 1
-- [CONVERT] Sybase UPDATE+INSERT → Snowflake MERGE
-- [CONVERT] ISNULL() → COALESCE()
-- [CONVERT] GETDATE() → CURRENT_TIMESTAMP()
-- ============================================================

MERGE INTO dim_product AS tgt
USING stg_products AS src
    ON tgt.product_code = src.product_code
WHEN MATCHED AND (
       tgt.product_name <> src.product_name
    OR tgt.lob_code     <> COALESCE(src.lob_code, tgt.lob_code)             -- [CONVERT] ISNULL → COALESCE
    OR tgt.is_active    <> src.active_flag
) THEN UPDATE SET
    tgt.product_name      = src.product_name,
    tgt.product_line      = COALESCE(src.product_line, 'UNKNOWN'),
    tgt.lob_code          = src.lob_code,
    tgt.lob_description   = CASE src.lob_code
                                WHEN 'AUTO' THEN 'Personal Auto'
                                WHEN 'HOME' THEN 'Homeowners'
                                WHEN 'COMM' THEN 'Commercial Lines'
                                WHEN 'LIFE' THEN 'Life and Annuity'
                                ELSE 'Other'
                            END,
    tgt.sub_lob_code      = src.sub_lob_code,
    tgt.coverage_type     = src.coverage_type,
    tgt.rating_plan       = src.rating_plan,
    tgt.min_premium       = src.min_premium,
    tgt.max_coverage      = src.max_coverage,
    tgt.is_active         = src.active_flag,
    tgt.effective_date    = src.effective_date,
    tgt.discontinue_date  = src.discontinue_date,
    tgt.dw_update_ts      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_code, product_name, product_line,
    lob_code, lob_description, sub_lob_code,
    coverage_type, rating_plan, min_premium, max_coverage,
    is_active, effective_date, discontinue_date
) VALUES (
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


-- ============================================================
-- SECTION 3: DIM_AGENT SCD Type 2
-- [CONVERT] Sybase UPDATE+INSERT two-step → Snowflake 3-step SCD2
--   Step A: Expire changed records (UPDATE)
--   Step B: Insert new versions + brand new agents (INSERT)
-- [CONVERT] DATEADD(DAY, -1, GETDATE()) → DATEADD(DAY, -1, CURRENT_DATE())
-- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
-- [CONVERT] UPDATE ... FROM → Snowflake UPDATE ... FROM syntax
-- ============================================================

-- Step A: Expire current records where attributes have changed
UPDATE dim_agent AS da
SET    da.scd_end_date      = DATEADD(DAY, -1, CURRENT_DATE()),              -- [CONVERT] GETDATE() → CURRENT_DATE()
       da.scd_current_flag  = 'N',
       da.dw_update_ts      = CURRENT_TIMESTAMP()
FROM   stg_agents sa
WHERE  da.agent_id = sa.agent_id
AND    da.scd_current_flag = 'Y'
AND (
       da.agency_id         <> COALESCE(sa.agency_id, -1)                     -- [CONVERT] ISNULL → COALESCE
    OR da.region_code       <> COALESCE(sa.region_code, 'UNKN')
    OR da.channel_code      <> COALESCE(sa.channel_code, 'UNKN')
    OR da.is_active         <> sa.active_flag
    OR da.license_expiry    <> COALESCE(sa.license_expiry, da.license_expiry)
);

-- Step B: Insert new version for changed agents + brand new agents
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
    CURRENT_DATE(),                                                           -- [CONVERT] CAST(GETDATE() AS DATE) → CURRENT_DATE()
    NULL,   -- current record
    'Y',
    COALESCE((                                                                -- [CONVERT] ISNULL → COALESCE
        SELECT MAX(scd_version) + 1
        FROM dim_agent x WHERE x.agent_id = sa.agent_id
    ), 1)
FROM stg_agents sa
WHERE NOT EXISTS (
    SELECT 1 FROM dim_agent da
    WHERE  da.agent_id          = sa.agent_id
    AND    da.scd_current_flag  = 'Y'
);


-- ============================================================
-- SECTION 4: DIM_CUSTOMER SCD Type 2
-- [CONVERT] Deep CASE nesting preserved (Snowflake compatible)
-- [CONVERT] DATEDIFF(YEAR, dob, GETDATE()) → DATEDIFF(YEAR, dob, CURRENT_DATE())
-- [CONVERT] String concatenation: + → ||
-- [CONVERT] LEFT(zip, 5) preserved
-- ============================================================

-- Step A: Expire records where tracked attributes changed
UPDATE dim_customer AS dc
SET    dc.scd_end_date     = DATEADD(DAY, -1, CURRENT_DATE()),
       dc.scd_current_flag = 'N',
       dc.dw_update_ts     = CURRENT_TIMESTAMP()
FROM   stg_customers sc
WHERE  dc.customer_id = sc.customer_id
AND    dc.scd_current_flag = 'Y'
AND (
       COALESCE(dc.state_code,    'XX')    <> COALESCE(sc.state_code,    'XX')
    OR COALESCE(dc.zip_code,      '00000') <> COALESCE(sc.zip_code,      '00000')
    OR COALESCE(dc.marital_status, 'X')    <> COALESCE(sc.marital_status, 'X')
    OR COALESCE(dc.credit_score,   0)      <> COALESCE(sc.credit_score,   0)
    OR COALESCE(dc.customer_segment, '')   <> COALESCE(sc.customer_segment, '')
);

-- Step B: Insert new / new-version customer records
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
    -- [CONVERT] String concatenation: + → ||
    LTRIM(RTRIM(COALESCE(sc.first_name, '') || ' ' || COALESCE(sc.last_name, ''))),
    sc.first_name,
    sc.last_name,
    sc.date_of_birth,
    -- Derive age band from DOB
    -- [CONVERT] DATEDIFF(YEAR, dob, GETDATE()) → DATEDIFF(YEAR, dob, CURRENT_DATE())
    CASE
        WHEN DATEDIFF(YEAR, sc.date_of_birth, CURRENT_DATE()) < 26 THEN '18-25'
        WHEN DATEDIFF(YEAR, sc.date_of_birth, CURRENT_DATE()) < 36 THEN '26-35'
        WHEN DATEDIFF(YEAR, sc.date_of_birth, CURRENT_DATE()) < 46 THEN '36-45'
        WHEN DATEDIFF(YEAR, sc.date_of_birth, CURRENT_DATE()) < 56 THEN '46-55'
        WHEN DATEDIFF(YEAR, sc.date_of_birth, CURRENT_DATE()) < 66 THEN '56-65'
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
    -- Region derived from state_code
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
    -- Credit band
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
    CURRENT_DATE(),
    NULL,
    'Y',
    COALESCE((
        SELECT MAX(scd_version) + 1
        FROM dim_customer x WHERE x.customer_id = sc.customer_id
    ), 1)
FROM stg_customers sc
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer dc
    WHERE  dc.customer_id      = sc.customer_id
    AND    dc.scd_current_flag = 'Y'
);

-- [REMOVE] PRINT statements removed (Sybase utility output)
-- Original: PRINT 'Dimension tables loaded successfully'
-- Original: PRINT 'dim_date    : ' + CONVERT(VARCHAR, (SELECT COUNT(*) FROM dim_date)) + ' rows'
-- etc.
