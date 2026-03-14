---
name: hipaa-phi-governance
description: "Implement HIPAA-compliant two-zone PHI architecture with automatic data classification, masking policies, RBAC, and offshore access controls. Use when: setting up healthcare data governance, enabling India/offshore team access, implementing PHI masking, configuring network policies, creating RBAC for healthcare. Triggers: HIPAA, PHI, masking, two-zone, offshore access, India access, data classification, PII, de-identified, safe harbor, healthcare governance."
---

# HIPAA PHI Governance

## Setup

**Query** `snowflake_product_docs` for:
- `CREATE MASKING POLICY syntax`
- `IS_ROLE_IN_SESSION function`
- `CLASSIFICATION_PROFILE syntax`
- `CREATE NETWORK POLICY syntax`

## Workflow

### Step 1: Environment Discovery

**Ask** user:
```
To set up HIPAA-compliant governance:

1. How is the data stored?
   a) Native Snowflake tables
   b) External tables (S3)

2. Where is the data located? (database.schema)

3. What was the original source system?
   a) EPIC Clarity
   b) Cerner Millennium
   c) Meditech
   d) Custom EHR/EDW

4. How was data ingested?
   a) IDMC → Snowflake
   b) IDMC → S3 → External Tables
   c) OpenFlow
   d) Other

5. Where should de-identified data be created? (database.schema for Zone 1)
```

**⚠️ STOP**: Wait for user response before proceeding.

### Step 2: Access Requirements

**Ask** user:
```
Access configuration:

1. Which locations need data access?
   a) US Only
   b) US + India
   c) US + Multiple Offshore

2. Do you have allowed IP ranges? (If yes, provide CIDR blocks)

3. Which roles need PHI access? (Select all that apply)
   - Data Engineer
   - Analyst
   - App Service Account
   - Admin
```

**⚠️ STOP**: Wait for user response.

### Step 3: PHI Type Identification

**Ask** user:
```
Which PHI types exist in your data? (Select all)
- [ ] MRN/Patient ID
- [ ] SSN
- [ ] Patient Name
- [ ] Date of Birth
- [ ] Address
- [ ] ZIP Code
- [ ] Phone
- [ ] Email
- [ ] Clinical Notes

Do you need partial masking for any fields?
- ZIP (show first 3 digits only)
- DOB (show year only)
- Name (show initials only)
```

**⚠️ STOP**: Wait for user response.

### Step 4: Generate DDL

**Generate** the following SQL based on user inputs:

#### 4.1 Database Structure
```sql
-- Zone 0: Raw PHI (US-only access)
CREATE DATABASE IF NOT EXISTS <zone0_database>;
CREATE SCHEMA IF NOT EXISTS <zone0_database>.<zone0_schema>;

-- Zone 1: De-identified (offshore access allowed)
CREATE DATABASE IF NOT EXISTS <zone1_database>;
CREATE SCHEMA IF NOT EXISTS <zone1_database>.<zone1_schema>;
```

#### 4.2 RBAC Roles
```sql
CREATE ROLE IF NOT EXISTS PHI_ACCESS_ROLE;
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS APP_SERVICE_ROLE;

-- Data Engineers inherit PHI access
GRANT ROLE PHI_ACCESS_ROLE TO ROLE DATA_ENGINEER_ROLE;

-- Zone 0 grants (PHI)
GRANT USAGE ON DATABASE <zone0_database> TO ROLE PHI_ACCESS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone0_database>.<zone0_schema> TO ROLE PHI_ACCESS_ROLE;

-- Zone 1 grants (Masked - offshore safe)
GRANT USAGE ON DATABASE <zone1_database> TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON DATABASE <zone1_database> TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone1_database>.<zone1_schema> TO ROLE DATA_ENGINEER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone1_database>.<zone1_schema> TO ROLE ANALYST_ROLE;
```

#### 4.3 Classification Profile
```sql
CREATE OR REPLACE CLASSIFICATION_PROFILE <project>_PHI_CLASSIFICATION
    MINIMUM_OBJECT_AGE_FOR_CLASSIFICATION_DAYS = 0
    MAXIMUM_CLASSIFICATION_VALIDITY_DAYS = 30
    AUTO_TAG = TRUE
    COMMENT = 'Automatic PHI/PII classification';

ALTER DATABASE <zone0_database> 
    SET CLASSIFICATION_PROFILE = <project>_PHI_CLASSIFICATION;
```

#### 4.4 Masking Policies

**Generate** policies based on selected PHI types:

```sql
-- Patient ID (hash-based)
CREATE OR REPLACE MASKING POLICY PHI_PATIENT_ID_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE 'MRN-' || SHA2(val, 256)::STRING END;

-- SSN (full mask)
CREATE OR REPLACE MASKING POLICY PHI_SSN_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE '***-**-****' END;

-- Name (redact)
CREATE OR REPLACE MASKING POLICY PHI_NAME_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE 'REDACTED' END;

-- DOB (year only)
CREATE OR REPLACE MASKING POLICY PHI_DOB_MASK AS (val DATE) 
RETURNS DATE ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE DATE_FROM_PARTS(YEAR(val), 1, 1) END;

-- ZIP (partial - Safe Harbor compliant)
CREATE OR REPLACE MASKING POLICY PHI_ZIP_PARTIAL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE LEFT(val, 3) || '**' END;

-- Contact info (full redact)
CREATE OR REPLACE MASKING POLICY PHI_CONTACT_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE '**REDACTED**' END;
```

#### 4.5 Network Policies (if offshore)

```sql
-- US-only for Zone 0
CREATE OR REPLACE NETWORK POLICY US_ONLY_ACCESS
    ALLOWED_IP_LIST = ('<us_ip_ranges>')
    COMMENT = 'Restrict PHI to US only';

-- India allowed for Zone 1
CREATE OR REPLACE NETWORK POLICY INDIA_ACCESS_ALLOWED
    ALLOWED_IP_LIST = ('<us_ip_ranges>', '<india_ip_ranges>')
    COMMENT = 'Allow offshore access to masked data';
```

### Step 5: Review and Approval

**Present** generated DDL to user:
```
I've generated the following:
1. Database/Schema DDL (Zone 0 and Zone 1)
2. RBAC roles and grants
3. Classification profile
4. Masking policies for: [list selected PHI types]
5. Network policies for: [US only / US + India]

Ready to execute? 
- [Yes] Execute all DDL
- [Review] Show me the full SQL first
- [Export] Save to file only
```

**⚠️ STOP**: Wait for user approval before executing any DDL.

### Step 6: Execute and Validate

**If approved**, execute DDL using `snowflake_sql_execute`.

**After execution**, run validation:
```sql
-- Test masking as PHI role
USE ROLE PHI_ACCESS_ROLE;
SELECT <masked_column> FROM <table> LIMIT 1;  -- Should see real data

-- Test masking as non-PHI role
USE ROLE ANALYST_ROLE;
SELECT <masked_column> FROM <table> LIMIT 1;  -- Should see masked data
```

**Present** validation results to user.

### Step 7: Review Classification Results

**After classification runs** (may take a few minutes):
```sql
SELECT TABLE_NAME, COLUMN_NAME, SEMANTIC_CATEGORY, PRIVACY_CATEGORY
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_CLASSIFICATION_LATEST
WHERE TABLE_SCHEMA = '<zone0_schema>'
ORDER BY TABLE_NAME, COLUMN_NAME;
```

**Present** classification results and recommend policy mappings.

## Stopping Points

- ✋ Step 1: After environment discovery
- ✋ Step 2: After access requirements
- ✋ Step 3: After PHI type identification
- ✋ Step 5: Before executing DDL
- ✋ Step 7: After classification results (for policy mapping review)

## Output

- Zone 0 and Zone 1 databases/schemas created
- RBAC roles configured with proper grants
- Classification profile enabled
- Masking policies created and applied
- Network policies configured (if offshore access)
- Validation queries confirming masking works

## Related Skills

- `cortex-ml-classification` - Use Zone 1 masked data for ML training
- `operational-action-queue` - Dashboard should connect to Zone 1 for offshore users
- `healthcare-analytics-accelerator` - Invokes this skill during Foundation phase
