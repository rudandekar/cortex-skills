# Example 03: COMPTIME Flat File + Oracle

## Overview

This example demonstrates a real-world ETL pattern with:
- Flat file source (fixed-width)
- Oracle database target
- Expression transformations (date conversions, string manipulation)
- Multiple source tables
- Complex field mappings

## Source Files

- `input/COMPTIME.xml` - Informatica PowerMart export

## Expected Output

### Decomposition

This workflow loads computed time data from a flat file into Oracle:
- Model: `mart_p1_computedtime`

### Model: `mart_p1_computedtime`

```sql
{{ config(
    materialized='table',
    schema='ADPCOMPPRD',
    tags=['wf_p1_computedtime', 'daily', 'payroll'],
    meta={
        'source_workflow': 'wf_P1_ComputedTime',
        'target_table': 'P1_COMPUTEDTIME',
        'source_type': 'flat_file',
        'generated_by': 'INFA2DBT_accelerator_v1.1'
    }
) }}

WITH source_comptime_file AS (
    -- Flat file source - requires Snowflake stage setup
    SELECT
        $1::VARCHAR(100) AS RAW_LINE,
        SUBSTR($1, 1, 9) AS EMPLOYEE_ID,
        SUBSTR($1, 10, 8) AS DATE_WORKED_STR,
        SUBSTR($1, 18, 5) AS HOURS_WORKED_STR,
        SUBSTR($1, 23, 3) AS PAY_CODE,
        SUBSTR($1, 26, 10) AS DEPARTMENT_CODE
    FROM {{ source('STAGING', 'COMPTIME_FLAT_FILE') }}
),

transformed_dates AS (
    SELECT
        EMPLOYEE_ID,
        TO_DATE(DATE_WORKED_STR, 'YYYYMMDD') AS DATE_WORKED,
        TRY_CAST(HOURS_WORKED_STR AS DECIMAL(5,2)) AS HOURS_WORKED,
        TRIM(PAY_CODE) AS PAY_CODE,
        TRIM(DEPARTMENT_CODE) AS DEPARTMENT_CODE,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    FROM source_comptime_file
),

validated_records AS (
    SELECT
        EMPLOYEE_ID,
        DATE_WORKED,
        HOURS_WORKED,
        PAY_CODE,
        DEPARTMENT_CODE,
        LOAD_TIMESTAMP
    FROM transformed_dates
    WHERE EMPLOYEE_ID IS NOT NULL
      AND DATE_WORKED IS NOT NULL
      AND HOURS_WORKED IS NOT NULL
),

final AS (
    SELECT
        EMPLOYEE_ID,
        DATE_WORKED,
        HOURS_WORKED,
        PAY_CODE,
        DEPARTMENT_CODE,
        LOAD_TIMESTAMP
    FROM validated_records
)

SELECT * FROM final
```

### schema.yml

```yaml
version: 2

models:
  - name: mart_p1_computedtime
    description: >
      Payroll computed time data loaded from flat file source.
      Converted from Informatica workflow wf_P1_ComputedTime.
      Original source: fixed-width flat file.
      Original target: Oracle ADPCOMPPRD.P1_COMPUTEDTIME.
    meta:
      source_workflow: wf_P1_ComputedTime
      target_table: P1_COMPUTEDTIME
      original_source_type: flat_file
      original_target_type: oracle
    columns:
      - name: EMPLOYEE_ID
        description: "Source: COMPTIME flat file positions 1-9."
        tests:
          - not_null
      - name: DATE_WORKED
        description: "Source: COMPTIME flat file positions 10-17, converted from YYYYMMDD."
        tests:
          - not_null
      - name: HOURS_WORKED
        description: "Source: COMPTIME flat file positions 18-22, converted to decimal."
        tests:
          - not_null
      - name: PAY_CODE
        description: "Source: COMPTIME flat file positions 23-25."
      - name: DEPARTMENT_CODE
        description: "Source: COMPTIME flat file positions 26-35."
      - name: LOAD_TIMESTAMP
        description: "System generated load timestamp."
        tests:
          - not_null
```

## Flat File Configuration

The source flat file requires Snowflake stage setup:

```sql
-- Create file format for fixed-width
CREATE OR REPLACE FILE FORMAT comptime_fixed_width
    TYPE = 'CSV'
    RECORD_DELIMITER = '\n'
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    TRIM_SPACE = FALSE;

-- Create stage
CREATE OR REPLACE STAGE comptime_stage
    FILE_FORMAT = comptime_fixed_width;

-- Create external table or load to staging
CREATE OR REPLACE TABLE STAGING.COMPTIME_FLAT_FILE (
    RAW_LINE VARCHAR(200)
);
```

## Transformation Chain

```
┌─────────────────────────┐
│ COMPTIME Flat File      │
│ (Fixed Width Source)    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ SQ_COMPTIME             │
│ (Source Qualifier)      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ EXP_Transform           │
│ (Expression)            │
│ - Date conversion       │
│ - Type casting          │
│ - Trimming              │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ FIL_Valid_Records       │
│ (Filter)                │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ P1_COMPUTEDTIME         │
│ (Oracle Target)         │
└─────────────────────────┘
```

## Migration Considerations

### Flat File Handling

Informatica flat file sources need special handling in Snowflake:

1. **Option A: Snowflake Stage** (recommended)
   - Upload files to internal/external stage
   - Use COPY INTO or external table

2. **Option B: Snowpipe**
   - Automated ingestion from cloud storage
   - Near real-time loading

3. **Option C: Pre-processing**
   - Convert flat file to CSV/Parquet
   - Load via standard DBT source

### Oracle Target Conversion

Oracle-specific syntax converted to Snowflake:

| Oracle | Snowflake |
|--------|-----------|
| `TO_DATE('str', 'fmt')` | `TO_DATE('str', 'fmt')` |
| `NVL(val, default)` | `NVL(val, default)` |
| `SYSDATE` | `CURRENT_TIMESTAMP()` |
| `SUBSTR(str, pos, len)` | `SUBSTR(str, pos, len)` |
| `TRIM(str)` | `TRIM(str)` |

## Notes

- Flat file source requires Snowflake stage configuration
- Fixed-width parsing done via SUBSTR functions
- Oracle target types mapped to Snowflake equivalents
- Consider adding data quality tests for format validation
