---
name: sensitive-data-governance
description: "Implement two-zone data architecture with automatic classification, masking policies, RBAC, and geographic access controls for any sensitive data (PII, PCI, confidential). Use when: setting up data governance, enabling offshore team access, implementing column masking, configuring network policies. Triggers: data governance, masking, two-zone, offshore access, PII, sensitive data, data classification, RBAC, network policy, geographic restriction."
---

# Sensitive Data Governance

Generic skill for protecting sensitive data with two-zone architecture, masking, and access controls.
Supports multiple compliance frameworks: HIPAA, PCI-DSS, GDPR, SOX, or custom policies.

## Setup

**Query** `snowflake_product_docs` for: `CREATE MASKING POLICY`, `IS_ROLE_IN_SESSION`, `CLASSIFICATION_PROFILE`, `CREATE NETWORK POLICY`.

## Workflow

### Step 1: Compliance Framework

**Ask** user: Which framework (HIPAA / PCI-DSS / GDPR / SOX / Custom / Multiple). If HIPAA, suggest switching to specialized `hipaa-phi-governance` skill.

**⚠️ STOP**: Wait for user response.

### Step 2: Environment Discovery

**Ask** user: Storage type (native / external tables), sensitive data location (database.schema), masked data target location, and data source type (transactional DB / SaaS / EDW / files).

**⚠️ STOP**: Wait for user response.

### Step 3: Access Requirements

**Ask** user: Geographic locations needing access (single / multiple-same / multiple-tiered), locations for masked-only access, IP ranges to allow/block, and roles needing unmasked access.

**⚠️ STOP**: Wait for user response.

### Step 4: Sensitive Data Types

Present options based on compliance framework:

**PCI-DSS:** PAN, Cardholder Name, Expiration Date, CVV (should not be stored!), PIN.

**GDPR:** Name, Email, Phone, Address, National ID, DOB, IP Address, Biometric data, Health data.

**Custom:** User specifies column patterns and masking type (FULL_REDACT / PARTIAL / HASH / NULL / CUSTOM).

**⚠️ STOP**: Wait for user response.

### Step 5: Generate DDL

#### 5.1 Two-Zone Database Structure
```sql
CREATE DATABASE IF NOT EXISTS <zone0_database>;  -- Sensitive (restricted)
CREATE SCHEMA IF NOT EXISTS <zone0_database>.<zone0_schema>;
CREATE DATABASE IF NOT EXISTS <zone1_database>;  -- Masked (broader access)
CREATE SCHEMA IF NOT EXISTS <zone1_database>.<zone1_schema>;
```

#### 5.2 RBAC Roles
```sql
CREATE ROLE IF NOT EXISTS SENSITIVE_DATA_ACCESS_ROLE COMMENT = 'Can view unmasked data';
CREATE ROLE IF NOT EXISTS DATA_STEWARD_ROLE COMMENT = 'Data governance and quality';
CREATE ROLE IF NOT EXISTS ANALYST_ROLE COMMENT = 'Analytics on masked data only';
CREATE ROLE IF NOT EXISTS SERVICE_ACCOUNT_ROLE COMMENT = 'Application service accounts';
GRANT ROLE SENSITIVE_DATA_ACCESS_ROLE TO ROLE DATA_STEWARD_ROLE;
```

Grant Zone 0 access to SENSITIVE_DATA_ACCESS_ROLE, Zone 1 access to ANALYST_ROLE and SERVICE_ACCOUNT_ROLE.

#### 5.3 Classification Profile
```sql
CREATE OR REPLACE CLASSIFICATION_PROFILE <project>_DATA_CLASSIFICATION
    MINIMUM_OBJECT_AGE_FOR_CLASSIFICATION_DAYS = 0
    MAXIMUM_CLASSIFICATION_VALIDITY_DAYS = 30
    AUTO_TAG = TRUE;
ALTER DATABASE <zone0_database> SET CLASSIFICATION_PROFILE = <project>_DATA_CLASSIFICATION;
```

#### 5.4 Masking Policies

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
```

Generate additional policies for phone, date, numeric, and PAN types following the same `IS_ROLE_IN_SESSION` pattern.

#### 5.5 Network Policies

If geographic restrictions needed, create network policies with ALLOWED_IP_LIST and BLOCKED_IP_LIST for sensitive data access control.

### Step 6: Review and Approval

Present summary of generated artifacts and get approval before executing.

**⚠️ STOP**: Wait for approval.

### Step 7: Execute and Validate

Execute DDL. Test as SENSITIVE_DATA_ACCESS_ROLE (should see real data) and as ANALYST_ROLE (should see masked data).

### Step 8: Classification Review

Query DATA_CLASSIFICATION_LATEST for automated classification results. Recommend additional policy mappings for any unmasked sensitive columns.

## Stopping Points

- ✋ Steps 1-4: After each discovery step
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
