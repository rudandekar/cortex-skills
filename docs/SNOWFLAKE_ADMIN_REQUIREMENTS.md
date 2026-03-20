# INFA2DBT Accelerator — Snowflake Environment Requirements

**Document Purpose:** Request for Snowflake environment provisioning  
**Project:** Informatica PowerCenter to dbt Migration  
**Date:** March 2026

---

## Overview

The INFA2DBT Accelerator requires a dedicated Snowflake database, schema, tables, and a Cortex Search service to manage pipeline state and enable AI-assisted code conversion. This document outlines the required Snowflake objects and privileges.

---

## 1. Required Snowflake Objects

### 1.1 Database and Schema

| Object | Name | Purpose |
|--------|------|---------|
| Database | `INFA2DBT_DB` | Dedicated database for migration pipeline |
| Schema | `INFA2DBT_DB.PIPELINE` | Contains all pipeline state tables |

### 1.2 Tables

| Table | Purpose |
|-------|---------|
| `PIPELINE_STATE` | Tracks pipeline run status and progress |
| `MODEL_REGISTRY` | Stores converted dbt models with SQL content |
| `FIDELITY_SCORES` | Tracks conversion quality metrics |
| `INFA2DBT_CORPUS` | RAG corpus with labelled conversion examples |

### 1.3 Cortex Search Service

| Object | Name | Purpose |
|--------|------|---------|
| Cortex Search Service | `INFA2DBT_CORPUS_SEARCH` | AI-powered pattern matching for code conversion |

### 1.4 Warehouse

| Object | Name | Size | Purpose |
|--------|------|------|---------|
| Warehouse | `INFA2DBT_WH` | MEDIUM (recommended) | Query execution and Cortex Search indexing |

### 1.5 Stage (Optional)

| Object | Name | Purpose |
|--------|------|---------|
| Internal Stage | `INFA_XMLS` | Upload Informatica XML files directly to Snowflake |

---

## 2. Roles Required

We request two roles be created:

### 2.1 Deployment Role (One-time Setup)

**Role Name:** `INFA2DBT_DEPLOYER`

Used by the deployment team to create initial infrastructure. Can be revoked after setup.

**Required Privileges:**

```sql
-- Account-level privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE INFA2DBT_DEPLOYER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE INFA2DBT_DEPLOYER;
```

### 2.2 Execution Role (Ongoing Operations)

**Role Name:** `INFA2DBT_EXECUTOR`

Used by the migration team to run the pipeline on an ongoing basis.

**Required Privileges:**

```sql
-- Database and schema access
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE INFA2DBT_EXECUTOR;
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_EXECUTOR;

-- Table access (read/write)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA INFA2DBT_DB.PIPELINE 
    TO ROLE INFA2DBT_EXECUTOR;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA INFA2DBT_DB.PIPELINE 
    TO ROLE INFA2DBT_EXECUTOR;

-- Warehouse access
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE INFA2DBT_EXECUTOR;

-- Cortex Search service access
GRANT USAGE ON CORTEX SEARCH SERVICE INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH 
    TO ROLE INFA2DBT_EXECUTOR;

-- Stage access (if using XML upload)
GRANT READ, WRITE ON STAGE INFA2DBT_DB.PIPELINE.INFA_XMLS 
    TO ROLE INFA2DBT_EXECUTOR;
```

---

## 3. Optional: ROI Analysis Access

For Phase 3 optimization analysis (Agent 6), the pipeline analyzes query patterns to prioritize optimization efforts. This requires read access to Snowflake usage views.

**Additional Privileges for `INFA2DBT_EXECUTOR`:**

```sql
-- Option A: Grant imported privileges (recommended)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE INFA2DBT_EXECUTOR;

-- Option B: Grant specific view access
GRANT SELECT ON VIEW SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
    TO ROLE INFA2DBT_EXECUTOR;
GRANT SELECT ON VIEW SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
    TO ROLE INFA2DBT_EXECUTOR;
```

**Note:** This is optional. The core conversion pipeline (Agents 1-5) works without ACCOUNT_USAGE access.

---

## 4. User Assignments

Please assign the following users to the roles:

| User | Role | Purpose |
|------|------|---------|
| `[DEPLOYMENT_USER]` | `INFA2DBT_DEPLOYER` | Initial setup (can be revoked after) |
| `[TEAM_MEMBER_1]` | `INFA2DBT_EXECUTOR` | Pipeline execution |
| `[TEAM_MEMBER_2]` | `INFA2DBT_EXECUTOR` | Pipeline execution |
| `[SERVICE_ACCOUNT]` | `INFA2DBT_EXECUTOR` | Automated execution (if applicable) |

---

## 5. Complete Setup Script

For convenience, here is the complete SQL script to provision the environment:

```sql
-- ============================================================
-- INFA2DBT ACCELERATOR — SNOWFLAKE PROVISIONING SCRIPT
-- Run as ACCOUNTADMIN or SYSADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create Roles
CREATE ROLE IF NOT EXISTS INFA2DBT_DEPLOYER;
CREATE ROLE IF NOT EXISTS INFA2DBT_EXECUTOR;

-- 2. Grant deployment privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE INFA2DBT_DEPLOYER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE INFA2DBT_DEPLOYER;

-- 3. Create Database and Schema
CREATE DATABASE IF NOT EXISTS INFA2DBT_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'INFA2DBT Accelerator - Informatica to dbt conversion pipeline';

CREATE SCHEMA IF NOT EXISTS INFA2DBT_DB.PIPELINE
    COMMENT = 'Pipeline state, model registry, and RAG corpus';

-- 4. Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS INFA2DBT_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for INFA2DBT pipeline operations';

-- 5. Create Tables
USE SCHEMA INFA2DBT_DB.PIPELINE;

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

-- 6. Create Stage (Optional)
CREATE STAGE IF NOT EXISTS INFA_XMLS
    COMMENT = 'Stage for Informatica XML workflow files';

-- 7. Grant Executor Role Privileges
GRANT USAGE ON DATABASE INFA2DBT_DB TO ROLE INFA2DBT_EXECUTOR;
GRANT USAGE ON SCHEMA INFA2DBT_DB.PIPELINE TO ROLE INFA2DBT_EXECUTOR;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA INFA2DBT_DB.PIPELINE 
    TO ROLE INFA2DBT_EXECUTOR;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA INFA2DBT_DB.PIPELINE 
    TO ROLE INFA2DBT_EXECUTOR;
GRANT USAGE ON WAREHOUSE INFA2DBT_WH TO ROLE INFA2DBT_EXECUTOR;
GRANT READ, WRITE ON STAGE INFA2DBT_DB.PIPELINE.INFA_XMLS TO ROLE INFA2DBT_EXECUTOR;

-- 8. (Optional) Grant ACCOUNT_USAGE access for ROI analysis
-- Uncomment if ROI scoring is required
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE INFA2DBT_EXECUTOR;

-- 9. Assign Users to Roles (update with actual usernames)
-- GRANT ROLE INFA2DBT_DEPLOYER TO USER <deployment_user>;
-- GRANT ROLE INFA2DBT_EXECUTOR TO USER <team_member_1>;
-- GRANT ROLE INFA2DBT_EXECUTOR TO USER <team_member_2>;

-- ============================================================
-- NOTE: Cortex Search Service will be created separately after
-- the INFA2DBT_CORPUS table is seeded with examples.
-- ============================================================
```

---

## 6. Post-Provisioning Steps

After the Snowflake admin provisions the environment:

1. **Seed the corpus table** — We will insert labelled conversion examples
2. **Create Cortex Search service** — Requires seeded corpus data
3. **Validate connectivity** — Test connection from execution environment
4. **Begin pipeline execution** — Start processing Informatica XMLs

---

## 7. Support Contact

For questions about these requirements:

| Contact | Role | Email |
|---------|------|-------|
| [Your Name] | Migration Lead | [email] |
| [Technical Lead] | Technical Architect | [email] |

---

## 8. Summary Checklist

| Item | Required | Status |
|------|----------|--------|
| Database `INFA2DBT_DB` | Yes | ☐ |
| Schema `PIPELINE` | Yes | ☐ |
| 4 State Tables | Yes | ☐ |
| Warehouse `INFA2DBT_WH` (MEDIUM) | Yes | ☐ |
| Role `INFA2DBT_DEPLOYER` | Yes | ☐ |
| Role `INFA2DBT_EXECUTOR` | Yes | ☐ |
| Stage `INFA_XMLS` | Optional | ☐ |
| ACCOUNT_USAGE access | Optional | ☐ |
| User assignments | Yes | ☐ |

---

*Document generated by INFA2DBT Accelerator v2.0.0*
