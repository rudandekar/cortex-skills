---
name: hipaa-phi-governance
description: "HIPAA-compliant PHI governance with two-zone architecture, Safe Harbor de-identification, masking policies, and offshore access controls. Use when: healthcare data governance, PHI protection, enabling India/offshore team access to healthcare data, HIPAA compliance. Triggers: HIPAA, PHI, healthcare governance, Safe Harbor, protected health information, EPIC, Cerner, EHR security, India access healthcare."
---

# HIPAA PHI Governance

Healthcare-specific extension of `sensitive-data-governance` with pre-configured PHI types,
Safe Harbor compliant masking patterns, and HIPAA §164.514 de-identification standards.

## Overview

This skill provides HIPAA-specific defaults on top of the generic `sensitive-data-governance` skill:
- **18 HIPAA Identifiers** pre-configured
- **Safe Harbor** compliant masking (ZIP → first 3 digits, DOB → year only)
- **PHI-specific roles** (PHI_ACCESS_ROLE, CLINICIAN_ROLE, etc.)
- **Healthcare system** patterns (EPIC, Cerner, Meditech)

## Setup

**Load** reference: `../generic/sensitive-data-governance/SKILL.md` for base patterns.

**Query** `snowflake_product_docs` for:
- `CREATE MASKING POLICY syntax`
- `HIPAA de-identification`
- `Safe Harbor method`

## Workflow

### Step 1: Healthcare Environment

**Ask** user:
```
Healthcare data configuration:

1. Source EHR system?
   a) EPIC Clarity
   b) Cerner Millennium
   c) Meditech
   d) AllScripts
   e) Custom/EDW

2. How is data ingested into Snowflake?
   a) IDMC → Snowflake
   b) IDMC → S3 → External Tables
   c) OpenFlow
   d) Other

3. Where is the PHI data located? (database.schema)

4. Where should de-identified data be created? (database.schema)
```

**⚠️ STOP**: Wait for user response.

### Step 2: Offshore Access

**Ask** user:
```
Offshore team access:

1. Are offshore teams involved?
   a) Yes - India
   b) Yes - Other location
   c) No - US only

2. If offshore, what IP ranges should be allowed for Zone 1 (masked) access?
   (Provide CIDR blocks or "configure later")

3. Should offshore teams have ANY access to Zone 0 (PHI)?
   a) No (recommended for HIPAA compliance)
   b) Yes - specific roles only (requires BAA and additional controls)
```

**⚠️ STOP**: Wait for user response.

### Step 3: PHI Types (18 HIPAA Identifiers)

**Present** HIPAA identifiers checklist:
```
Select PHI types present in your data (18 HIPAA Identifiers):

Direct Identifiers:
- [x] Names (PAT_NAME, PATIENT_NAME)
- [x] Geographic data smaller than state (ADDRESS, ZIP)
- [x] Dates (DOB, ADMISSION_DATE, DISCHARGE_DATE, DEATH_DATE)
- [x] Phone numbers
- [x] Fax numbers
- [ ] Email addresses
- [x] SSN
- [x] Medical record numbers (MRN, PAT_ID, PAT_MRN_ID)
- [ ] Health plan beneficiary numbers
- [ ] Account numbers
- [ ] Certificate/license numbers
- [ ] Vehicle identifiers
- [ ] Device identifiers
- [ ] URLs
- [ ] IP addresses
- [ ] Biometric identifiers
- [ ] Full-face photos
- [ ] Any other unique identifier

Pre-selected items are common in EHR systems. Adjust as needed.
```

**⚠️ STOP**: Wait for user response.

### Step 4: Generate HIPAA-Compliant DDL

#### 4.1 Two-Zone Architecture
```sql
-- Zone 0: PHI (US-only, BAA-covered access)
CREATE DATABASE IF NOT EXISTS <zone0_database>
    COMMENT = 'PHI Zone - HIPAA Protected';
CREATE SCHEMA IF NOT EXISTS <zone0_database>.<source>_RAW;

-- Zone 1: De-identified (Safe Harbor compliant, offshore-safe)
CREATE DATABASE IF NOT EXISTS <zone1_database>
    COMMENT = 'De-identified Zone - Safe Harbor Compliant';
CREATE SCHEMA IF NOT EXISTS <zone1_database>.<source>_MASKED;
```

#### 4.2 Healthcare RBAC Roles
```sql
-- PHI access roles
CREATE ROLE IF NOT EXISTS PHI_ACCESS_ROLE
    COMMENT = 'Can view unmasked PHI - requires BAA coverage';
CREATE ROLE IF NOT EXISTS CLINICIAN_ROLE
    COMMENT = 'Clinical users with treatment purpose';
CREATE ROLE IF NOT EXISTS HEALTH_IT_ADMIN_ROLE
    COMMENT = 'IT administrators with PHI access';

-- Non-PHI roles (offshore-safe)
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE
    COMMENT = 'Data engineering on de-identified data';
CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Analytics on de-identified data';
CREATE ROLE IF NOT EXISTS APP_SERVICE_ROLE
    COMMENT = 'Application service accounts';

-- Inheritance
GRANT ROLE PHI_ACCESS_ROLE TO ROLE CLINICIAN_ROLE;
GRANT ROLE PHI_ACCESS_ROLE TO ROLE HEALTH_IT_ADMIN_ROLE;

-- Zone 0 grants (PHI - US only)
GRANT USAGE ON DATABASE <zone0_database> TO ROLE PHI_ACCESS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone0_database>.<source>_RAW 
    TO ROLE PHI_ACCESS_ROLE;

-- Zone 1 grants (De-identified - offshore OK)
GRANT USAGE ON DATABASE <zone1_database> TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON DATABASE <zone1_database> TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone1_database>.<source>_MASKED 
    TO ROLE DATA_ENGINEER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <zone1_database>.<source>_MASKED 
    TO ROLE ANALYST_ROLE;
```

#### 4.3 HIPAA Classification Profile
```sql
CREATE OR REPLACE CLASSIFICATION_PROFILE HIPAA_PHI_CLASSIFICATION
    MINIMUM_OBJECT_AGE_FOR_CLASSIFICATION_DAYS = 0
    MAXIMUM_CLASSIFICATION_VALIDITY_DAYS = 30
    AUTO_TAG = TRUE
    COMMENT = 'HIPAA PHI/PII automatic classification';

ALTER DATABASE <zone0_database> 
    SET CLASSIFICATION_PROFILE = HIPAA_PHI_CLASSIFICATION;
```

#### 4.4 Safe Harbor Compliant Masking Policies

```sql
-- MRN/Patient ID (hash-based - preserves joins)
CREATE OR REPLACE MASKING POLICY PHI_MRN_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE 'MRN-' || LEFT(SHA2(val || '<salt>', 256), 12) END;

-- SSN (full redaction)
CREATE OR REPLACE MASKING POLICY PHI_SSN_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE '***-**-****' END;

-- Patient Name (full redaction)
CREATE OR REPLACE MASKING POLICY PHI_NAME_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE 'REDACTED' END;

-- DOB (Safe Harbor: year only for age < 90)
CREATE OR REPLACE MASKING POLICY PHI_DOB_MASK AS (val DATE) 
RETURNS DATE ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
        WHEN DATEDIFF('year', val, CURRENT_DATE()) >= 90 THEN NULL  -- Age 90+ grouped
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)  -- Year only
    END;

-- ZIP Code (Safe Harbor: first 3 digits, unless population < 20k)
CREATE OR REPLACE MASKING POLICY PHI_ZIP_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
        WHEN LEFT(val, 3) IN ('036','059','063','102','203','556','692','790','821','823','830','831','878','879','884','890','893') 
            THEN '000**'  -- Low-population ZIPs masked completely
        ELSE LEFT(val, 3) || '**'
    END;

-- Address (full redaction)
CREATE OR REPLACE MASKING POLICY PHI_ADDRESS_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE 'REDACTED' END;

-- Phone/Fax (full redaction)
CREATE OR REPLACE MASKING POLICY PHI_PHONE_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE '***-***-****' END;

-- Email (full redaction)
CREATE OR REPLACE MASKING POLICY PHI_EMAIL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE '***@***.***' END;

-- Dates (admission, discharge, etc. - shift by random offset)
CREATE OR REPLACE MASKING POLICY PHI_DATE_SHIFT_MASK AS (val DATE) 
RETURNS DATE ->
    CASE WHEN IS_ROLE_IN_SESSION('PHI_ACCESS_ROLE') THEN val
         ELSE DATEADD('day', -ABS(HASH(val) % 365), val) END;
```

#### 4.5 Network Policies (Offshore Restrictions)
```sql
-- US-only access for Zone 0 (PHI)
CREATE OR REPLACE NETWORK POLICY US_ONLY_PHI_ACCESS
    ALLOWED_IP_LIST = ('<us_office_cidr>', '<us_vpn_cidr>')
    COMMENT = 'HIPAA: Restrict PHI access to US locations';

-- Allow offshore for Zone 1 (de-identified)
CREATE OR REPLACE NETWORK POLICY OFFSHORE_DEIDENTIFIED_ACCESS
    ALLOWED_IP_LIST = ('<us_cidr>', '<india_cidr>')
    COMMENT = 'Safe Harbor compliant de-identified data access';
```

### Step 5: EHR-Specific Column Mappings

**Based on EHR system**, provide column → policy mappings:

**EPIC Clarity:**
| Column Pattern | Masking Policy |
|---------------|----------------|
| PAT_MRN_ID, PAT_ID | PHI_MRN_MASK |
| PAT_NAME, PATIENT_NAME | PHI_NAME_MASK |
| BIRTH_DATE, DOB | PHI_DOB_MASK |
| SSN, SS_NUM | PHI_SSN_MASK |
| ADD_LINE_1, ADDRESS | PHI_ADDRESS_MASK |
| ZIP, POSTAL_CODE | PHI_ZIP_MASK |
| HOME_PHONE, WORK_PHONE | PHI_PHONE_MASK |
| EMAIL_ADDRESS | PHI_EMAIL_MASK |

**Cerner Millennium:**
| Column Pattern | Masking Policy |
|---------------|----------------|
| PERSON_ID, MRN | PHI_MRN_MASK |
| NAME_FULL_FORMATTED | PHI_NAME_MASK |
| BIRTH_DT_TM | PHI_DOB_MASK |

### Step 6: Review and Execute

**Present** summary and get approval.

**⚠️ STOP**: Wait for approval before executing.

### Step 7: Validate Safe Harbor Compliance

```sql
-- Verify masking works
USE ROLE PHI_ACCESS_ROLE;
SELECT PAT_MRN_ID, PAT_NAME, BIRTH_DATE, ZIP FROM <table> LIMIT 3;
-- Should see: real values

USE ROLE ANALYST_ROLE;
SELECT PAT_MRN_ID, PAT_NAME, BIRTH_DATE, ZIP FROM <table> LIMIT 3;
-- Should see: MRN-xxxx, REDACTED, 1985-01-01, 100**

-- Verify Zone 1 is Safe Harbor compliant
-- No direct identifiers should be visible to non-PHI roles
```

## Stopping Points

- ✋ Step 1: After healthcare environment config
- ✋ Step 2: After offshore access config
- ✋ Step 3: After PHI type selection
- ✋ Step 6: Before executing DDL
- ✋ Step 7: After validation

## Output

- HIPAA-compliant two-zone architecture
- Healthcare-specific RBAC roles
- Safe Harbor compliant masking policies
- EHR-specific column mappings
- Network policies for offshore restrictions
- Validation confirming de-identification

## HIPAA Reference

**Safe Harbor De-identification (§164.514(b)(2)):**
- Remove all 18 identifier types
- ZIP: First 3 digits only (unless population < 20,000)
- Dates: Year only for DOB; may shift other dates
- Age: Group 90+ years together

**Expert Determination (§164.514(b)(1)):**
- Alternative method requiring statistical expert certification
- Not covered by this skill

## Related Skills

- `sensitive-data-governance` - Generic base skill (PCI, GDPR, SOX)
- `healthcare-analytics-accelerator` - Invokes this skill during Foundation phase
- `cortex-ml-classification` - Use Zone 1 for ML training
