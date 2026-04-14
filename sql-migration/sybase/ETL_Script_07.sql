-- =============================================================================
-- ETL SCRIPT 07: Rules, Defaults, and IQ-Specific Index Patterns
-- Reference file for all RULE/DEFAULT bindings and IQ index creation
-- Complexity  : SIMPLE-MEDIUM — Reference DDL patterns
-- =============================================================================

-- =============================================================================
-- SECTION 1: Additional domain-specific RULE objects
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_pct_range' AND type = 'R')
    DROP RULE rule_pct_range
GO
CREATE RULE rule_pct_range
AS @value >= 0.0000 AND @value <= 1.0000    -- Percentage stored as decimal 0-1
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_gender_code' AND type = 'R')
    DROP RULE rule_gender_code
GO
CREATE RULE rule_gender_code
AS @value IN ('M', 'F', 'X', 'U')   -- Male, Female, Non-binary, Unknown
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_claim_type' AND type = 'R')
    DROP RULE rule_claim_type
GO
CREATE RULE rule_claim_type
AS @value IN ('BI','PD','COMP','COLL','MED','UM','UIM','PIP','GL','PROP','LIAB','OTHER')
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'rule_treaty_type' AND type = 'R')
    DROP RULE rule_treaty_type
GO
CREATE RULE rule_treaty_type
AS @value IN ('QS','XL','FAC','STOP_LOSS','CAT_XL')
GO

-- =============================================================================
-- SECTION 2: Additional DEFAULT objects
-- =============================================================================

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_unknown_str' AND type = 'D')
    DROP DEFAULT def_unknown_str
GO
CREATE DEFAULT def_unknown_str
AS 'UNKNOWN'
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_zero_int' AND type = 'D')
    DROP DEFAULT def_zero_int
GO
CREATE DEFAULT def_zero_int
AS 0
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'def_far_future' AND type = 'D')
    DROP DEFAULT def_far_future
GO
CREATE DEFAULT def_far_future
AS '9999-12-31'
GO

-- Bind additional rules and defaults to reinsurance staging
EXEC sp_bindrule    'rule_pct_range',       'stg_reinsurance.reinsurer_share_pct'
EXEC sp_bindrule    'rule_treaty_type',     'stg_reinsurance.treaty_type'
EXEC sp_bindrule    'rule_yn_flag',         'stg_reinsurance.reinstatement_flag'
EXEC sp_binddefault 'def_zero_int',         'stg_reinsurance.layer_number'
GO

-- Bind gender rule to customers
EXEC sp_bindrule    'rule_gender_code',     'stg_customers.gender_code'
GO

-- Bind claim type rule to claims staging
EXEC sp_bindrule    'rule_claim_type',      'stg_claims.claim_type_code'
GO

-- =============================================================================
-- SECTION 3: IQ-Specific Index Creation
-- IQ UNIQUE cardinality hints — no equivalent in Snowflake
-- Snowflake uses micro-partitioning and CLUSTER BY instead
-- =============================================================================

-- Bitmap-style index on low-cardinality columns (IQ optimized)
CREATE INDEX idx_fact_policy_status
    ON fact_policy (status_code)
    IQ UNIQUE (8)          -- Only 8 distinct status codes
GO

CREATE INDEX idx_fact_policy_product
    ON fact_policy (product_key)
    IQ UNIQUE (500)
GO

CREATE INDEX idx_fact_claims_type
    ON fact_claims (claim_type_code)
    IQ UNIQUE (12)
GO

CREATE INDEX idx_fact_claims_status
    ON fact_claims (status_code)
    IQ UNIQUE (8)
GO

CREATE INDEX idx_agg_policy_year_month
    ON agg_policy_monthly (year_number, month_number)
    IQ UNIQUE (240)       -- 20 years * 12 months
GO

CREATE INDEX idx_dim_customer_segment
    ON dim_customer (customer_segment)
    IQ UNIQUE (10)
GO

-- =============================================================================
-- SECTION 4: SET TEMPORARY OPTION settings for query optimization
-- IQ-specific session-level tuning — no Snowflake equivalent
-- Snowflake handles optimization automatically via query optimizer
-- =============================================================================

SET TEMPORARY OPTION Query_Plan_As_Html = 'OFF'
GO
SET TEMPORARY OPTION Query_Rows_Returned_Limit = '0'   -- No row limit
GO
SET TEMPORARY OPTION Query_Plan_After_Run = 'OFF'
GO
SET TEMPORARY OPTION Forced_Predicate_Pushdown = 'ALL'
GO
SET TEMPORARY OPTION Hash_Pinnable_Cache = '75'         -- % of cache for hash joins
GO

