---
name: sensitive-data-governance
description: "Implement two-zone data architecture with automatic classification, masking policies, RBAC, and geographic access controls for any sensitive data (PII, PCI, confidential). Use when: setting up data governance, enabling offshore team access, implementing column masking, configuring network policies. Triggers: data governance, masking, two-zone, offshore access, PII, sensitive data, data classification, RBAC, network policy, geographic restriction."
---

# Sensitive Data Governance

Generic skill for protecting sensitive data with two-zone architecture, masking, and access controls.
Supports multiple compliance frameworks: HIPAA, PCI-DSS, GDPR, SOX, or custom policies.

## Setup

**Query** `snowflake_product_docs` for:
- `CREATE MASKING POLICY syntax`
- `IS_ROLE_IN_SESSION function`
- `CLASSIFICATION_PROFILE syntax`
- `CREATE NETWORK POLICY syntax`

## Workflow

### Step 1: Compliance Framework

**Ask** user:
```
What compliance framework applies to this data?

a) HIPAA (healthcare PHI)
b) PCI-DSS (payment card data)
c) GDPR (EU personal data)
d) SOX (financial data)
e) Custom / Internal policy
f) Multiple frameworks
```

**If HIPAA selected:**
```
Consider using the specialized `hipaa-phi-governance` skill which has
pre-configured PHI types and Safe Harbor compliant masking patterns.

Continue with generic skill? [Yes / Switch to hipaa-phi-governance]
```

**⚠️ STOP**: Wait for user response.

### Step 2: Environment Discovery

**Ask** user:
```
Data environment configuration:

1. How is the data stored?
   a) Native Snowflake tables
   b) External tables (S3/Azure/GCS)

2. Where is the sensitive data located? (database.schema)

3. Where should masked/de-identified data be created? (database.schema)

4. Data source type?
   a) Transactional database (Oracle, SQL Server, PostgreSQL)
   b) SaaS application (Salesforce, Workday, etc.)
   c) Data warehouse / EDW
   d) Files (CSV, Parquet, JSON)
   e) Other
```

**⚠️ STOP**: Wait for user response.

### Step 3: Access Requirements

**Ask** user:
```
Access configuration:

1. Which geographic locations need data access?
   a) Single country (specify)
   b) Multiple countries - same access level
   c) Multiple countries - tiered access (some see masked only)

2. Locations that should see MASKED data only:
   (e.g., India, Philippines, or "None")

3. Do you have specific IP ranges to allow/block?
   a) Yes (provide CIDR blocks)
   b) No (configure later)

4. What roles need access to UNMASKED sensitive data?
   (e.g., Data Steward, Compliance Officer, specific service accounts)
```

**⚠️ STOP**: Wait for user response.

### Step 4: Sensitive Data Types

**Present** options based on compliance framework:

**For PCI-DSS:**
```
Select sensitive data types present:
- [ ] Primary Account Number (PAN)
- [ ] Cardholder Name
- [ ] Expiration Date
- [ ] CVV/CVC (should not be stored!)
- [ ] PIN
```

**For GDPR:**
```
Select personal data types present:
- [ ] Name
- [ ] Email
- [ ] Phone
- [ ] Address
- [ ] National ID (SSN, etc.)
- [ ] Date of Birth
- [ ] IP Address
- [ ] Biometric data
- [ ] Health data (triggers HIPAA-level controls)
```

**For Custom:**
```
List your sensitive data types and masking requirements:
- Column pattern or name:
- Masking type: [FULL_REDACT | PARTIAL | HASH | NULL | CUSTOM]
```

**⚠️ STOP**: Wait for user response.

### Step 5: Generate DDL

**Generate** based on inputs:

#### 5.1 Two-Zone Database Structure
```sql
-- Zone 0: Sensitive data (restricted access)
CREATE DATABASE IF NOT EXISTS <zone0_database>;
CREATE SCHEMA IF NOT EXISTS <zone0_database>.<zone0_schema>;

-- Zone 1: Masked data (broader access, offshore-safe)
CREATE DATABASE IF NOT EXISTS <zone1_database>;
CREATE SCHEMA IF NOT EXISTS <zone1_database>.<zone1_schema>;
```

#### 5.2 RBAC Roles
```sql
-- Role hierarchy
CREATE ROLE IF NOT EXISTS SENSITIVE_DATA_ACCESS_ROLE
    COMMENT = 'Can view unmasked sensitive data';
CREATE ROLE IF NOT EXISTS DATA_STEWARD_ROLE
    COMMENT = 'Data governance and quality';
CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Analytics on masked data only';
CREATE ROLE IF NOT EXISTS SERVICE_ACCOUNT_ROLE
    COMMENT = 'Application service accounts';

-- Inheritance
GRANT ROLE SENSITIVE_DATA_ACCESS_ROLE TO ROLE DATA_STEWARD_ROLE;

-- Zone 0 access (sensitive - restricted)
GRANT USAGE ON DATABASE <zone0_database> TO ROLE SENSITIVE_DATA_ACCESS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone0_database>.<zone0_schema> 
    TO ROLE SENSITIVE_DATA_ACCESS_ROLE;

-- Zone 1 access (masked - broader)
GRANT USAGE ON DATABASE <zone1_database> TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE <zone1_database> TO ROLE SERVICE_ACCOUNT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone1_database>.<zone1_schema> 
    TO ROLE ANALYST_ROLE;
```

#### 5.3 Classification Profile
```sql
CREATE OR REPLACE CLASSIFICATION_PROFILE <project>_DATA_CLASSIFICATION
    MINIMUM_OBJECT_AGE_FOR_CLASSIFICATION_DAYS = 0
    MAXIMUM_CLASSIFICATION_VALIDITY_DAYS = 30
    AUTO_TAG = TRUE
    COMMENT = 'Automatic sensitive data classification';

ALTER DATABASE <zone0_database> 
    SET CLASSIFICATION_PROFILE = <project>_DATA_CLASSIFICATION;
```

#### 5.4 Masking Policies

**Generate** based on selected data types:

```sql
-- Full redaction (names, addresses, free text)
CREATE OR REPLACE MASKING POLICY MASK_FULL_REDACT AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE '***REDACTED***' END;

-- Hash-based (IDs - preserves referential integrity)
CREATE OR REPLACE MASKING POLICY MASK_HASH AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE 'ID-' || LEFT(SHA2(val, 256), 16) END;

-- Email masking (preserve domain)
CREATE OR REPLACE MASKING POLICY MASK_EMAIL AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE CONCAT('***@', SPLIT_PART(val, '@', 2)) END;

-- Phone masking (last 4 digits)
CREATE OR REPLACE MASKING POLICY MASK_PHONE AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4)) END;

-- Date masking (year only)
CREATE OR REPLACE MASKING POLICY MASK_DATE_YEAR AS (val DATE) 
RETURNS DATE ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE DATE_FROM_PARTS(YEAR(val), 1, 1) END;

-- Numeric masking (nullify)
CREATE OR REPLACE MASKING POLICY MASK_NUMERIC_NULL AS (val NUMBER) 
RETURNS NUMBER ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE NULL END;

-- PCI: Card number masking (first 6, last 4)
CREATE OR REPLACE MASKING POLICY MASK_PAN AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('SENSITIVE_DATA_ACCESS_ROLE') THEN val
         ELSE CONCAT(LEFT(val, 6), '******', RIGHT(val, 4)) END;
```

#### 5.5 Network Policies (if geographic restrictions)
```sql
CREATE OR REPLACE NETWORK POLICY RESTRICTED_ACCESS
    ALLOWED_IP_LIST = ('<allowed_ip_ranges>')
    BLOCKED_IP_LIST = ('<blocked_ip_ranges>')
    COMMENT = 'Geographic access restriction for sensitive data';

-- Apply to sensitive data role
ALTER USER <service_user> SET NETWORK_POLICY = RESTRICTED_ACCESS;
```

### Step 6: Review and Approval

**Present** summary:
```
Generated artifacts:
1. Two-zone database structure (Zone 0: sensitive, Zone 1: masked)
2. RBAC roles: SENSITIVE_DATA_ACCESS_ROLE, DATA_STEWARD_ROLE, ANALYST_ROLE
3. Classification profile with AUTO_TAG
4. Masking policies for: [list data types]
5. Network policies: [if configured]

Ready to execute?
- [Yes] Execute all DDL
- [Review] Show full SQL
- [Export] Save to file
```

**⚠️ STOP**: Wait for approval.

### Step 7: Execute and Validate

**Execute** DDL using `snowflake_sql_execute`.

**Run** validation:
```sql
-- Test as sensitive data role (should see real data)
USE ROLE SENSITIVE_DATA_ACCESS_ROLE;
SELECT <sensitive_column> FROM <table> LIMIT 1;

-- Test as analyst role (should see masked data)
USE ROLE ANALYST_ROLE;
SELECT <sensitive_column> FROM <table> LIMIT 1;
```

**Present** validation results.

### Step 8: Classification Review

**Query** classification results:
```sql
SELECT TABLE_NAME, COLUMN_NAME, SEMANTIC_CATEGORY, PRIVACY_CATEGORY
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_CLASSIFICATION_LATEST
WHERE DATABASE_NAME = '<zone0_database>'
ORDER BY TABLE_NAME, COLUMN_NAME;
```

**Present** findings and recommend additional policy mappings.

## Stopping Points

- ✋ Step 1: After compliance framework selection
- ✋ Step 2: After environment discovery
- ✋ Step 3: After access requirements
- ✋ Step 4: After sensitive data type selection
- ✋ Step 6: Before executing DDL
- ✋ Step 8: After classification review

## Output

- Two-zone database/schema structure
- RBAC roles with appropriate grants
- Classification profile (AUTO_TAG enabled)
- Masking policies for selected data types
- Network policies (if geographic restrictions)
- Validation queries confirming masking works

## Compliance Framework Reference

| Framework | Key Data Types | Special Requirements |
|-----------|---------------|---------------------|
| HIPAA | PHI (MRN, SSN, DOB, Address) | Safe Harbor de-identification |
| PCI-DSS | PAN, CVV, Cardholder Name | CVV must not be stored post-auth |
| GDPR | Any EU personal data | Right to erasure, data portability |
| SOX | Financial records | Audit trail, access logging |

## Related Skills

- `hipaa-phi-governance` - Healthcare-specific extension with PHI patterns
- `cortex-ml-classification` - Use Zone 1 masked data for ML training
- `operational-action-queue` - Dashboards should connect to Zone 1 for restricted users
