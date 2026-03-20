# INFA2DBT Accelerator — Roles and Privileges

This document specifies all Snowflake roles and privileges required to run the INFA2DBT Accelerator.

---

## Overview

The INFA2DBT Accelerator requires privileges at multiple levels:

| Level | Purpose |
|-------|---------|
| Database | Access to INFA2DBT_DB |
| Schema | Access to PIPELINE schema |
| Tables | Read/write state and registry tables |
| Cortex Search | Query the corpus search service |
| Warehouse | Compute for queries and Cortex Search |
| Cortex Functions | Access to SEARCH_PREVIEW function |

---

## Recommended Role Structure

```
ACCOUNTADMIN
    └── SYSADMIN
            └── INFA2DBT_ADMIN_ROLE      (Full admin access)
                    └── INFA2DBT_USER_ROLE   (Execution access)
```

---

## Option 1: Use Existing Role

Grant privileges to an existing role (e.g., `DATA_ENGINEERING_ROLE`):

```sql
USE ROLE ACCOUNTADMIN;

-- Database access
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE DATA_ENGINEERING_ROLE;

-- Schema access
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;

-- Table privileges
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA INFA2DBT_DB.PIPELINE TO ROLE DATA_ENGINEERING_ROLE;

-- Cortex Search service
GRANT USAGE ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH TO ROLE DATA_ENGINEERING_ROLE;

-- Warehouse
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE DATA_ENGINEERING_ROLE;

-- Stage (if using)
GRANT READ, WRITE ON STAGE INFA2DBT_DB.PIPELINE.INFA_XMLS TO ROLE DATA_ENGINEERING_ROLE;

-- Cortex functions (for SEARCH_PREVIEW)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DATA_ENGINEERING_ROLE;
```

---

## Option 2: Create Dedicated Roles

### Create Admin Role

```sql
USE ROLE ACCOUNTADMIN;

-- Create admin role for INFA2DBT management
CREATE ROLE IF NOT EXISTS INFA2DBT_ADMIN_ROLE
    COMMENT = 'Admin role for INFA2DBT Accelerator - can create/modify objects';

-- Grant to SYSADMIN hierarchy
GRANT ROLE INFA2DBT_ADMIN_ROLE TO ROLE SYSADMIN;

-- Database ownership
GRANT OWNERSHIP ON DATABASE INFA2DBT_DB TO ROLE INFA2DBT_ADMIN_ROLE;

-- Schema ownership
GRANT OWNERSHIP ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_ADMIN_ROLE;

-- All privileges on schema objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_ADMIN_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_ADMIN_ROLE;

-- Cortex Search admin
GRANT ALL PRIVILEGES ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH TO ROLE INFA2DBT_ADMIN_ROLE;

-- Warehouse admin
GRANT ALL PRIVILEGES ON WAREHOUSE INFA2DBT_WH TO ROLE INFA2DBT_ADMIN_ROLE;

-- Create Cortex Search services
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_ADMIN_ROLE;

-- Cortex functions
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE INFA2DBT_ADMIN_ROLE;
```

### Create User Role

```sql
-- Create user role for pipeline execution
CREATE ROLE IF NOT EXISTS INFA2DBT_USER_ROLE
    COMMENT = 'User role for INFA2DBT Accelerator - can execute pipeline';

-- Grant to admin role hierarchy
GRANT ROLE INFA2DBT_USER_ROLE TO ROLE INFA2DBT_ADMIN_ROLE;

-- Database access
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE INFA2DBT_USER_ROLE;

-- Schema access
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_USER_ROLE;

-- Table privileges (read/write for state tables)
GRANT SELECT, INSERT, UPDATE ON TABLE INFA2DBT_DB.PIPELINE.PIPELINE_STATE TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE INFA2DBT_DB.PIPELINE.MODEL_REGISTRY TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE INFA2DBT_DB.PIPELINE.FIDELITY_SCORES TO ROLE INFA2DBT_USER_ROLE;

-- Corpus table (read-only for users, admin adds examples)
GRANT SELECT ON TABLE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS TO ROLE INFA2DBT_USER_ROLE;

-- Cortex Search service
GRANT USAGE ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH TO ROLE INFA2DBT_USER_ROLE;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE INFA2DBT_USER_ROLE;

-- Stage access (if using)
GRANT READ ON STAGE INFA2DBT_DB.PIPELINE.INFA_XMLS TO ROLE INFA2DBT_USER_ROLE;

-- Cortex functions
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE INFA2DBT_USER_ROLE;
```

### Assign Roles to Users

```sql
-- Grant user role to specific users
GRANT ROLE INFA2DBT_USER_ROLE TO USER data_engineer_1;
GRANT ROLE INFA2DBT_USER_ROLE TO USER data_engineer_2;

-- Grant admin role to team lead
GRANT ROLE INFA2DBT_ADMIN_ROLE TO USER team_lead;
```

---

## Privilege Reference

### Required Privileges by Object

| Object | Privilege | Purpose |
|--------|-----------|---------|
| `INFA2DBT_DB` | USAGE | Access database |
| `INFA2DBT_DB.PIPELINE` | USAGE | Access schema |
| `PIPELINE_STATE` | SELECT, INSERT, UPDATE | Track pipeline runs |
| `MODEL_REGISTRY` | SELECT, INSERT, UPDATE | Store converted models |
| `FIDELITY_SCORES` | SELECT, INSERT, UPDATE | Record quality scores |
| `INFA2DBT_CORPUS` | SELECT | Query corpus examples |
| `INFA2DBT_CORPUS` | INSERT (admin only) | Add new examples |
| `INFA2DBT_CORPUS_SEARCH` | USAGE | Query Cortex Search |
| `INFA2DBT_WH` | USAGE | Execute queries |
| `INFA_XMLS` (stage) | READ, WRITE | Upload XML files |
| `SNOWFLAKE.CORTEX_USER` | Database role | Use SEARCH_PREVIEW |

### Cortex-Specific Privileges

The Cortex Search functionality requires:

```sql
-- Required for SEARCH_PREVIEW function
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;

-- Alternative: Grant specific function usage
GRANT USAGE ON FUNCTION SNOWFLAKE.CORTEX.SEARCH_PREVIEW(VARCHAR, VARCHAR) TO ROLE <your_role>;
```

---

## Verification Queries

### Check Role Grants

```sql
-- See all grants to a role
SHOW GRANTS TO ROLE INFA2DBT_USER_ROLE;

-- See grants on specific object
SHOW GRANTS ON TABLE INFA2DBT_DB.PIPELINE.MODEL_REGISTRY;

-- See grants on Cortex Search service
SHOW GRANTS ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH;
```

### Test Access

```sql
-- Switch to user role
USE ROLE INFA2DBT_USER_ROLE;
USE WAREHOUSE INFA2DBT_WH;

-- Test table access
SELECT COUNT(*) FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE;
SELECT COUNT(*) FROM INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS;

-- Test Cortex Search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
        '{"query": "filter", "columns": ["TRANSFORMATION_TYPE"], "limit": 1}'
    )
)['results'];

-- Test insert (should work)
INSERT INTO INFA2DBT_DB.PIPELINE.PIPELINE_STATE (RUN_ID, WORKFLOW_NAME, STATUS)
VALUES ('test_run', 'test_workflow', 'PENDING');

-- Cleanup test
DELETE FROM INFA2DBT_DB.PIPELINE.PIPELINE_STATE WHERE RUN_ID = 'test_run';
```

---

## Minimum Privilege Set

For the most restrictive setup, these are the absolute minimum privileges:

```sql
-- Minimum for pipeline execution
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE INFA2DBT_USER_ROLE;
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE INFA2DBT_DB.PIPELINE.PIPELINE_STATE TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE INFA2DBT_DB.PIPELINE.MODEL_REGISTRY TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT, INSERT ON TABLE INFA2DBT_DB.PIPELINE.FIDELITY_SCORES TO ROLE INFA2DBT_USER_ROLE;
GRANT SELECT ON TABLE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS TO ROLE INFA2DBT_USER_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH TO ROLE INFA2DBT_USER_ROLE;
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE INFA2DBT_USER_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE INFA2DBT_USER_ROLE;
```

---

## Network Policies (If Applicable)

If the client uses network policies, ensure the execution environment has access:

```sql
-- Check current network policies
SHOW NETWORK POLICIES;

-- Add IP ranges if needed (example)
ALTER NETWORK POLICY client_policy 
    SET ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8');
```

---

## Service Account Setup (For Automated Runs)

For scheduled or automated pipeline runs:

```sql
-- Create service account user
CREATE USER IF NOT EXISTS INFA2DBT_SERVICE_ACCOUNT
    PASSWORD = '<strong_password>'
    DEFAULT_ROLE = INFA2DBT_USER_ROLE
    DEFAULT_WAREHOUSE = INFA2DBT_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for automated INFA2DBT runs';

-- Grant role
GRANT ROLE INFA2DBT_USER_ROLE TO USER INFA2DBT_SERVICE_ACCOUNT;

-- For key-pair authentication (recommended)
ALTER USER INFA2DBT_SERVICE_ACCOUNT SET RSA_PUBLIC_KEY = '<public_key>';
```

---

## Audit Trail

The pipeline automatically logs to state tables, but for additional auditing:

```sql
-- Enable access history (if not already)
ALTER ACCOUNT SET ENABLE_INTERNAL_STAGES_PRIVATELINK = TRUE;

-- Query access history for INFA2DBT objects
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND ARRAY_CONTAINS('INFA2DBT_DB'::VARIANT, BASE_OBJECTS_ACCESSED:objectName)
ORDER BY QUERY_START_TIME DESC;
```

---

## Troubleshooting Permission Issues

### "Insufficient privileges" Error

```sql
-- Check what privileges are missing
USE ROLE ACCOUNTADMIN;
SHOW GRANTS TO ROLE <failing_role>;

-- Compare with required privileges above
-- Grant missing privileges
```

### "Object does not exist or not authorized"

```sql
-- Verify object exists
SHOW TABLES IN SCHEMA INFA2DBT_DB.PIPELINE;
SHOW CORTEX SEARCH SERVICES IN INFA2DBT_DB.PIPELINE;

-- Check if role has USAGE on database/schema
SHOW GRANTS TO ROLE <your_role>;
```

### Cortex Search "Access denied"

```sql
-- Ensure Cortex database role is granted
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;

-- Verify service grant
SHOW GRANTS ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH;
```
