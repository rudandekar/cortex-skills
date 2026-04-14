# Safe Harbor Compliant Masking Policies

HIPAA §164.514(b)(2) Safe Harbor de-identification masking policies for Snowflake.

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
