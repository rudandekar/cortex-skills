---
name: hipaa-phi-governance
description: "HIPAA-compliant PHI governance with two-zone architecture, Safe Harbor de-identification, masking policies, and offshore access controls. Use when: healthcare data governance, PHI protection, enabling India/offshore team access to healthcare data, HIPAA compliance. Triggers: HIPAA, PHI, healthcare governance, Safe Harbor, protected health information, EPIC, Cerner, EHR security, India access healthcare."
---

# HIPAA PHI Governance

Healthcare-specific extension of `sensitive-data-governance` with pre-configured PHI types,
Safe Harbor compliant masking patterns, and HIPAA §164.514 de-identification standards.

## Overview

- **18 HIPAA Identifiers** pre-configured
- **Safe Harbor** compliant masking (ZIP → first 3 digits, DOB → year only)
- **PHI-specific roles** (PHI_ACCESS_ROLE, CLINICIAN_ROLE, etc.)
- **Healthcare system** patterns (EPIC, Cerner, Meditech)

## Setup

**Load** reference: `../generic/sensitive-data-governance/SKILL.md` for base patterns.

## Workflow

### Step 1: Healthcare Environment

**Ask** user: Source EHR system (EPIC/Cerner/Meditech/AllScripts/Custom), data ingestion method (IDMC/OpenFlow/Other), PHI data location (database.schema), and de-identified data target location.

**⚠️ STOP**: Wait for user response.

### Step 2: Offshore Access

**Ask** user: Offshore team involvement (Yes-India / Yes-Other / No-US only), IP ranges for Zone 1 access, whether offshore should have ANY Zone 0 (PHI) access (not recommended).

**⚠️ STOP**: Wait for user response.

### Step 3: PHI Types (18 HIPAA Identifiers)

Present checklist of 18 HIPAA identifiers with common EHR defaults pre-selected: Names, Geographic data < state, Dates (DOB, admission, discharge, death), Phone, Fax, Email, SSN, MRN/PAT_ID, Health plan numbers, Account numbers, Certificate/license numbers, Vehicle IDs, Device IDs, URLs, IP addresses, Biometric IDs, Full-face photos, Other unique IDs.

**⚠️ STOP**: Wait for user response.

### Step 4: Generate HIPAA-Compliant DDL

#### 4.1 Two-Zone Architecture
```sql
CREATE DATABASE IF NOT EXISTS <zone0_database> COMMENT = 'PHI Zone - HIPAA Protected';
CREATE SCHEMA IF NOT EXISTS <zone0_database>.<source>_RAW;
CREATE DATABASE IF NOT EXISTS <zone1_database> COMMENT = 'De-identified Zone - Safe Harbor Compliant';
CREATE SCHEMA IF NOT EXISTS <zone1_database>.<source>_MASKED;
```

#### 4.2 Healthcare RBAC Roles
```sql
CREATE ROLE IF NOT EXISTS PHI_ACCESS_ROLE COMMENT = 'Can view unmasked PHI - requires BAA';
CREATE ROLE IF NOT EXISTS CLINICIAN_ROLE COMMENT = 'Clinical users with treatment purpose';
CREATE ROLE IF NOT EXISTS HEALTH_IT_ADMIN_ROLE COMMENT = 'IT administrators with PHI access';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE COMMENT = 'Data engineering on de-identified data';
CREATE ROLE IF NOT EXISTS ANALYST_ROLE COMMENT = 'Analytics on de-identified data';
GRANT ROLE PHI_ACCESS_ROLE TO ROLE CLINICIAN_ROLE;
GRANT ROLE PHI_ACCESS_ROLE TO ROLE HEALTH_IT_ADMIN_ROLE;
```

Grant Zone 0 access to PHI_ACCESS_ROLE, Zone 1 access to DATA_ENGINEER_ROLE and ANALYST_ROLE.

#### 4.3 HIPAA Classification Profile
```sql
CREATE OR REPLACE CLASSIFICATION_PROFILE HIPAA_PHI_CLASSIFICATION
    MINIMUM_OBJECT_AGE_FOR_CLASSIFICATION_DAYS = 0
    MAXIMUM_CLASSIFICATION_VALIDITY_DAYS = 30
    AUTO_TAG = TRUE;
ALTER DATABASE <zone0_database> SET CLASSIFICATION_PROFILE = HIPAA_PHI_CLASSIFICATION;
```

#### 4.4 Safe Harbor Compliant Masking Policies

**Load** references/safe-harbor-policies.md for the full set of Safe Harbor compliant masking policy DDL.

#### 4.5 Network Policies (Offshore Restrictions)

Create US-only network policy for Zone 0 (PHI) access and a broader policy allowing offshore IPs for Zone 1 (de-identified) access.

### Step 5: EHR-Specific Column Mappings

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

For Cerner Millennium: map PERSON_ID/MRN→PHI_MRN_MASK, NAME_FULL_FORMATTED→PHI_NAME_MASK, BIRTH_DT_TM→PHI_DOB_MASK.

### Step 6: Review and Execute

Present summary and get approval before executing DDL.

**⚠️ STOP**: Wait for approval.

### Step 7: Validate Safe Harbor Compliance

Test as PHI_ACCESS_ROLE (should see real data) and as ANALYST_ROLE (should see masked values: MRN-xxxx, REDACTED, year-only DOB, 3-digit ZIP).

## Stopping Points

- ✋ Step 1-3: After each discovery step
- ✋ Step 6: Before executing DDL
- ✋ Step 7: After validation

## Output

- HIPAA-compliant two-zone architecture
- Healthcare-specific RBAC roles
- Safe Harbor compliant masking policies
- EHR-specific column mappings
- Network policies for offshore restrictions

## HIPAA Reference

**Safe Harbor De-identification (§164.514(b)(2)):** Remove all 18 identifier types. ZIP: first 3 digits only (unless population < 20,000). Dates: year only for DOB. Age: group 90+ together.

## Related Skills

- `sensitive-data-governance` - Generic base skill (PCI, GDPR, SOX)
- `healthcare-analytics-accelerator` - Invokes this skill during Foundation phase
- `cortex-ml-classification` - Use Zone 1 for ML training
