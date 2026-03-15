# INFA2DBT Accelerator — Migration Guide

This guide provides step-by-step instructions to deploy the INFA2DBT Accelerator in a client Snowflake environment.

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN or equivalent privileges
- Warehouse with at least MEDIUM size (for Cortex Search)
- Cortex Code CLI installed (for skill execution)
- Git (for cloning repository)

---

## Step 1: Create Database and Schema

```sql
-- Connect as ACCOUNTADMIN or SYSADMIN
USE ROLE ACCOUNTADMIN;

-- Create dedicated database
CREATE DATABASE IF NOT EXISTS INFA2DBT_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'INFA2DBT Accelerator - Informatica to DBT conversion pipeline';

-- Create pipeline schema
CREATE SCHEMA IF NOT EXISTS INFA2DBT_DB.PIPELINE
    COMMENT = 'Pipeline state, model registry, and RAG corpus';

-- Grant usage to appropriate roles (adjust role name as needed)
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE DATA_ENGINEERING_ROLE;
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;
```

---

## Step 2: Create State Tables

```sql
USE SCHEMA INFA2DBT_DB.PIPELINE;

-- Pipeline run tracking
CREATE TABLE IF NOT EXISTS PIPELINE_STATE (
    RUN_ID VARCHAR(64) PRIMARY KEY,
    WORKFLOW_NAME VARCHAR(256) NOT NULL,
    XML_FILE_PATH VARCHAR(1024),
    STATUS VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    CURRENT_PHASE VARCHAR(64),
    CURRENT_GATE VARCHAR(32),
    STARTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT TIMESTAMP_NTZ,
    ERROR_MSG TEXT,
    RETRY_COUNT INTEGER DEFAULT 0,
    METADATA VARIANT,
    CREATED_BY VARCHAR(128) DEFAULT CURRENT_USER(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Model registry
CREATE TABLE IF NOT EXISTS MODEL_REGISTRY (
    MODEL_ID VARCHAR(64) PRIMARY KEY,
    RUN_ID VARCHAR(64) NOT NULL,
    MODEL_NAME VARCHAR(256) NOT NULL,
    SOURCE_WORKFLOW VARCHAR(256),
    SOURCE_MAPPING VARCHAR(256),
    TARGET_TABLE VARCHAR(256),
    TARGET_SCHEMA VARCHAR(256),
    SQL_CONTENT TEXT,
    SCHEMA_YML TEXT,
    UNIT_TEST_YML TEXT,
    TRANSFORMATION_TYPES ARRAY,
    COLUMN_COUNT INTEGER,
    CTE_COUNT INTEGER,
    FIDELITY_STATUS VARCHAR(32) DEFAULT 'PENDING',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fidelity scores
CREATE TABLE IF NOT EXISTS FIDELITY_SCORES (
    SCORE_ID VARCHAR(64) PRIMARY KEY,
    MODEL_ID VARCHAR(64) NOT NULL,
    RUN_ID VARCHAR(64) NOT NULL,
    OVERALL_SCORE FLOAT,
    STRUCTURE_SCORE FLOAT,
    SEMANTICS_SCORE FLOAT,
    TEST_COVERAGE FLOAT,
    TRANSFORMATION_COVERAGE FLOAT,
    COLUMN_MAPPING_ACCURACY FLOAT,
    LINEAGE_PRESERVED BOOLEAN DEFAULT TRUE,
    FLAGGED_ISSUES ARRAY,
    REVIEWER_NOTES TEXT,
    REVIEWED_BY VARCHAR(128),
    REVIEWED_AT TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAG corpus table
CREATE TABLE IF NOT EXISTS INFA2DBT_CORPUS (
    EXAMPLE_ID VARCHAR(64) PRIMARY KEY,
    TRANSFORMATION_TYPE VARCHAR(64) NOT NULL,
    INFA_PATTERN TEXT NOT NULL,
    DBT_PATTERN TEXT NOT NULL,
    DESCRIPTION TEXT NOT NULL,
    TAGS ARRAY,
    COMPLEXITY VARCHAR(16) DEFAULT 'MEDIUM',
    USE_CASE VARCHAR(128),
    EDGE_CASES TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## Step 3: Create Warehouse (if needed)

```sql
-- Create dedicated warehouse for INFA2DBT
CREATE WAREHOUSE IF NOT EXISTS INFA2DBT_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for INFA2DBT Cortex Search and pipeline operations';

-- Grant usage
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE DATA_ENGINEERING_ROLE;
```

---

## Step 4: Seed Corpus with Base Examples

```sql
USE SCHEMA INFA2DBT_DB.PIPELINE;

INSERT INTO INFA2DBT_CORPUS 
(EXAMPLE_ID, TRANSFORMATION_TYPE, INFA_PATTERN, DBT_PATTERN, DESCRIPTION, TAGS, COMPLEXITY, USE_CASE)
SELECT 'EXP001', 'Expression', 
'<TRANSFORMATION TYPE="Expression"><TABLEATTRIBUTE EXPRESSION="qty * price"/></TRANSFORMATION>',
'SELECT qty * price AS total FROM source_cte',
'Convert Informatica Expression to SQL calculation. Map input ports to columns, output ports to aliases.',
ARRAY_CONSTRUCT('calculation', 'derived_column', 'arithmetic'),
'LOW', 'Simple arithmetic'
UNION ALL
SELECT 'EXP002', 'Expression',
'<TRANSFORMATION TYPE="Expression"><TABLEATTRIBUTE EXPRESSION="IIF(status=A,Active,Inactive)"/></TRANSFORMATION>',
'SELECT CASE WHEN status = ''A'' THEN ''Active'' ELSE ''Inactive'' END AS status_desc FROM source_cte',
'Convert IIF expressions to SQL CASE statements for conditional logic.',
ARRAY_CONSTRUCT('conditional', 'case', 'iif'),
'MEDIUM', 'Conditional mapping'
UNION ALL
SELECT 'FLT001', 'Filter',
'<TRANSFORMATION TYPE="Filter"><TABLEATTRIBUTE NAME="FILTERCONDITION" VALUE="status = A"/></TRANSFORMATION>',
'SELECT * FROM source_cte WHERE status = ''A''',
'Filter transformation becomes WHERE clause.',
ARRAY_CONSTRUCT('filter', 'where', 'condition'),
'LOW', 'Row filtering'
UNION ALL
SELECT 'AGG001', 'Aggregator',
'<TRANSFORMATION TYPE="Aggregator"><TABLEATTRIBUTE NAME="GROUP BY" VALUE="region"/><TABLEATTRIBUTE EXPRESSION="SUM(amount)"/></TRANSFORMATION>',
'SELECT region, SUM(amount) AS total FROM source_cte GROUP BY region',
'Aggregator with GROUP BY ports and aggregate functions.',
ARRAY_CONSTRUCT('aggregation', 'group_by', 'sum'),
'MEDIUM', 'Data summarization'
UNION ALL
SELECT 'JNR001', 'Joiner',
'<TRANSFORMATION TYPE="Joiner"><TABLEATTRIBUTE NAME="Join Type" VALUE="Normal Join"/></TRANSFORMATION>',
'SELECT m.*, d.* FROM master_cte m INNER JOIN detail_cte d ON m.id = d.id',
'Normal Join becomes INNER JOIN. Master is left side.',
ARRAY_CONSTRUCT('join', 'inner_join'),
'MEDIUM', 'Table joining'
UNION ALL
SELECT 'JNR002', 'Joiner',
'<TRANSFORMATION TYPE="Joiner"><TABLEATTRIBUTE NAME="Join Type" VALUE="Master Outer Join"/></TRANSFORMATION>',
'SELECT m.*, d.* FROM master_cte m LEFT OUTER JOIN detail_cte d ON m.id = d.id',
'Master Outer Join becomes LEFT OUTER JOIN.',
ARRAY_CONSTRUCT('join', 'left_join', 'outer'),
'MEDIUM', 'Outer joining'
UNION ALL
SELECT 'LKP001', 'Lookup',
'<TRANSFORMATION TYPE="Lookup"><TABLEATTRIBUTE NAME="Lookup SQL Override" VALUE="SELECT id, name FROM dim_table"/></TRANSFORMATION>',
'WITH lkp AS (SELECT id, name FROM dim_table) SELECT src.*, lkp.name FROM source_cte src LEFT JOIN lkp ON src.id = lkp.id',
'Lookup becomes LEFT JOIN CTE. SQL Override defines CTE.',
ARRAY_CONSTRUCT('lookup', 'left_join', 'dimension'),
'HIGH', 'Dimension lookup'
UNION ALL
SELECT 'SQ001', 'Source Qualifier',
'<TRANSFORMATION TYPE="Source Qualifier"><TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM orders WHERE date >= SYSDATE - 30"/></TRANSFORMATION>',
'WITH source AS (SELECT * FROM {{ source(''raw'', ''orders'') }} WHERE date >= DATEADD(day, -30, CURRENT_DATE()))',
'Source Qualifier SQL becomes source CTE. Convert SYSDATE to CURRENT_DATE().',
ARRAY_CONSTRUCT('source', 'sql_override'),
'MEDIUM', 'Custom source query'
UNION ALL
SELECT 'SEQ001', 'Sequence Generator',
'<TRANSFORMATION TYPE="Sequence Generator"><TABLEATTRIBUTE NAME="Start Value" VALUE="1"/></TRANSFORMATION>',
'SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq_id, * FROM source_cte',
'Sequence Generator becomes ROW_NUMBER() window function.',
ARRAY_CONSTRUCT('sequence', 'surrogate_key'),
'MEDIUM', 'Key generation'
UNION ALL
SELECT 'SRT001', 'Sorter',
'<TRANSFORMATION TYPE="Sorter"><TABLEATTRIBUTE NAME="Sorter Data Order" VALUE="date DESC"/></TRANSFORMATION>',
'SELECT * FROM source_cte ORDER BY date DESC',
'Sorter becomes ORDER BY clause.',
ARRAY_CONSTRUCT('sort', 'order_by'),
'LOW', 'Data ordering'
UNION ALL
SELECT 'RTR001', 'Router',
'<TRANSFORMATION TYPE="Router"><GROUP NAME="GRP_NORTH" CONDITION="region = NORTH"/></TRANSFORMATION>',
'-- Create separate models: mart_north.sql with WHERE region = ''NORTH''',
'Router creates multiple outputs. Create separate DBT models per route.',
ARRAY_CONSTRUCT('router', 'conditional', 'branching'),
'HIGH', 'Conditional routing'
UNION ALL
SELECT 'UNI001', 'Union',
'<TRANSFORMATION TYPE="Union"><UNIONGROUP NAME="SET1"/><UNIONGROUP NAME="SET2"/></TRANSFORMATION>',
'SELECT * FROM set1_cte UNION ALL SELECT * FROM set2_cte',
'Union becomes UNION ALL. Ensure column alignment.',
ARRAY_CONSTRUCT('union', 'combine'),
'LOW', 'Data combining';
```

---

## Step 5: Create Cortex Search Service

```sql
-- Create the search service
CREATE OR REPLACE CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH
ON DESCRIPTION
ATTRIBUTES TRANSFORMATION_TYPE, COMPLEXITY, USE_CASE
WAREHOUSE = INFA2DBT_WH
TARGET_LAG = '1 hour'
AS SELECT 
    EXAMPLE_ID,
    TRANSFORMATION_TYPE,
    INFA_PATTERN,
    DBT_PATTERN,
    DESCRIPTION,
    COMPLEXITY,
    USE_CASE
FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS;

-- Verify service is active
SHOW CORTEX SEARCH SERVICES IN INFA2DBT_DB.PIPELINE;
```

---

## Step 6: Create Stage for XML Files (Optional)

```sql
-- Create internal stage for XML uploads
CREATE STAGE IF NOT EXISTS INFA2DBT_DB.PIPELINE.INFA_XMLS
    COMMENT = 'Stage for Informatica XML workflow files';

-- Grant access
GRANT READ, WRITE ON STAGE INFA2DBT_DB.PIPELINE.INFA_XMLS TO ROLE DATA_ENGINEERING_ROLE;
```

---

## Step 7: Clone Skills Repository

```bash
# Clone the INFA2DBT skills repository
git clone https://github.com/your-org/infa2dbt-accelerator.git

# Copy skills to Cortex Code location
cp -r infa2dbt-accelerator/skills/* ~/.snowflake/cortex/skills/
```

---

## Step 8: Configure Connection

Update your Cortex Code connection to point to the client environment:

```bash
# List available connections
cortex connections list

# Set active connection
cortex connections set CLIENT_SNOWFLAKE_CONNECTION
```

Or create a new connection in `~/.snowflake/connections.toml`:

```toml
[connections.CLIENT_ENV]
account = "client_account.region"
user = "your_user"
authenticator = "externalbrowser"
warehouse = "INFA2DBT_WH"
database = "INFA2DBT_DB"
schema = "PIPELINE"
role = "DATA_ENGINEERING_ROLE"
```

---

## Step 9: Verify Installation

```sql
-- Check tables exist
SELECT TABLE_NAME, ROW_COUNT 
FROM INFA2DBT_DB.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'PIPELINE';

-- Check Cortex Search service
SHOW CORTEX SEARCH SERVICES IN INFA2DBT_DB.PIPELINE;

-- Test corpus search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
        '{"query": "aggregation sum", "columns": ["TRANSFORMATION_TYPE", "DBT_PATTERN"], "limit": 2}'
    )
)['results'];
```

---

## Step 10: Grant Permissions

See `ROLES_AND_PRIVILEGES.md` for detailed permission requirements.

Quick setup:

```sql
-- Grant all required privileges to the execution role
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH TO ROLE DATA_ENGINEERING_ROLE;
```

---

## Post-Migration Tasks

1. **Add Client-Specific Corpus Examples**
   - Review client's Informatica workflows for unique patterns
   - Add labelled examples to `INFA2DBT_CORPUS` table

2. **Create Coding Guidelines**
   - Customize `coding-guidelines-template.md` for client standards
   - Save as `docs/INFA2DBT_coding_guidelines.md` in DBT project

3. **Test with Sample Workflow**
   ```
   Run INFA2DBT Agent 1 on /path/to/sample_workflow.xml
   ```

4. **Configure Resource Monitors (Optional)**
   ```sql
   CREATE RESOURCE MONITOR INFA2DBT_MONITOR
       WITH CREDIT_QUOTA = 100
       TRIGGERS ON 75 PERCENT DO NOTIFY
                ON 90 PERCENT DO NOTIFY
                ON 100 PERCENT DO SUSPEND;
   
   ALTER WAREHOUSE INFA2DBT_WH SET RESOURCE_MONITOR = INFA2DBT_MONITOR;
   ```

---

## Troubleshooting

### Cortex Search Service Not Active

```sql
-- Check service status
DESCRIBE CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH;

-- If stuck in BUILDING, check warehouse
ALTER WAREHOUSE INFA2DBT_WH RESUME;
```

### Permission Denied Errors

```sql
-- Verify grants
SHOW GRANTS TO ROLE DATA_ENGINEERING_ROLE;

-- Re-grant if needed
GRANT ALL PRIVILEGES ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;
```

### Corpus Search Returns No Results

```sql
-- Verify corpus has data
SELECT COUNT(*) FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS;

-- Check TARGET_LAG hasn't expired (service may not have refreshed)
-- Force refresh by recreating service if needed
```

---

## Rollback

To remove INFA2DBT objects:

```sql
-- Drop Cortex Search service first
DROP CORTEX SEARCH SERVICE IF EXISTS INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH;

-- Drop tables
DROP TABLE IF EXISTS INFA2DBT_DB.PIPELINE.FIDELITY_SCORES;
DROP TABLE IF EXISTS INFA2DBT_DB.PIPELINE.MODEL_REGISTRY;
DROP TABLE IF EXISTS INFA2DBT_DB.PIPELINE.PIPELINE_STATE;
DROP TABLE IF EXISTS INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS;

-- Drop schema and database
DROP SCHEMA IF EXISTS INFA2DBT_DB.PIPELINE;
DROP DATABASE IF EXISTS INFA2DBT_DB;

-- Drop warehouse (if dedicated)
DROP WAREHOUSE IF EXISTS INFA2DBT_WH;
```
