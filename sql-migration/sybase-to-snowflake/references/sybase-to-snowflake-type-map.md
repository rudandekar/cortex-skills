# Sybase-to-Snowflake Type Map

> Reference for the `sybase-to-snowflake` skill v0.3.0. Consulted during Step 5 (Target Generator).
> Organized into three sections: Data Types, Functions, and Patterns.

---

## Table of Contents

1. [Data Type Mappings](#1-data-type-mappings)
2. [Function Translations](#2-function-translations)
3. [Pattern Conversions](#3-pattern-conversions)
4. [System Object Replacements](#4-system-object-replacements)
5. [Batch and Control Flow](#5-batch-and-control-flow)
6. [String and Date Format Codes](#6-string-and-date-format-codes)

---

## 1. Data Type Mappings

### Numeric Types

| Sybase Type | Snowflake Type | Notes |
|-------------|---------------|-------|
| `TINYINT` | `SMALLINT` | Snowflake has no TINYINT; SMALLINT is the smallest integer type |
| `SMALLINT` | `SMALLINT` | Direct mapping |
| `INT` / `INTEGER` | `INT` | Direct mapping |
| `BIGINT` | `BIGINT` | Direct mapping |
| `NUMERIC(p,s)` | `NUMBER(p,s)` | Direct mapping; preserve precision and scale |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | Snowflake uses NUMBER for both |
| `FLOAT` | `FLOAT` | Direct mapping; beware floating-point comparison in reconciliation |
| `REAL` | `FLOAT` | Sybase REAL maps to Snowflake FLOAT |
| `DOUBLE PRECISION` | `DOUBLE` | Direct mapping |
| `MONEY` | `NUMBER(19,4)` | No native MONEY type in Snowflake |
| `SMALLMONEY` | `NUMBER(10,4)` | No native SMALLMONEY type in Snowflake |
| `BIT` | `BOOLEAN` | Sybase BIT (0/1) maps to Snowflake BOOLEAN (TRUE/FALSE); adjust comparisons accordingly |

### String Types

| Sybase Type | Snowflake Type | Notes |
|-------------|---------------|-------|
| `CHAR(n)` | `CHAR(n)` | Direct mapping; Snowflake max is 16,777,216 |
| `VARCHAR(n)` | `VARCHAR(n)` | Direct mapping |
| `NCHAR(n)` | `CHAR(n)` | Snowflake stores all strings as UTF-8 natively |
| `NVARCHAR(n)` | `VARCHAR(n)` | Snowflake stores all strings as UTF-8 natively |
| `TEXT` | `VARCHAR(16777216)` | Snowflake has no TEXT type; use max VARCHAR |
| `NTEXT` | `VARCHAR(16777216)` | Same as TEXT |
| `UNICHAR(n)` | `CHAR(n)` | Sybase ASE Unicode; Snowflake is UTF-8 native |
| `UNIVARCHAR(n)` | `VARCHAR(n)` | Sybase ASE Unicode; Snowflake is UTF-8 native |

### Date/Time Types

| Sybase Type | Snowflake Type | Notes |
|-------------|---------------|-------|
| `DATETIME` | `TIMESTAMP_NTZ` | Sybase DATETIME has ~3.33ms precision; TIMESTAMP_NTZ has nanosecond precision |
| `SMALLDATETIME` | `TIMESTAMP_NTZ` | Sybase SMALLDATETIME has 1-minute precision; use TIMESTAMP_NTZ |
| `DATE` | `DATE` | Direct mapping (Sybase ASE 15.7+, Sybase IQ) |
| `TIME` | `TIME` | Direct mapping (Sybase ASE 15.7+, Sybase IQ) |
| `TIMESTAMP` | `TIMESTAMP_NTZ` | Sybase TIMESTAMP is an auto-incrementing binary; convert carefully — if used as a version column, consider using a `NUMBER` with a sequence instead |
| `BIGDATETIME` | `TIMESTAMP_NTZ(9)` | Sybase IQ; microsecond precision maps to nanosecond |
| `BIGTIME` | `TIME(9)` | Sybase IQ |

### Binary Types

| Sybase Type | Snowflake Type | Notes |
|-------------|---------------|-------|
| `BINARY(n)` | `BINARY(n)` | Direct mapping |
| `VARBINARY(n)` | `VARBINARY(n)` | Direct mapping |
| `IMAGE` | `BINARY` | Snowflake max BINARY is 8MB; for larger BLOBs use internal stages |

### Special Types

| Sybase Type | Snowflake Type | Notes |
|-------------|---------------|-------|
| `IDENTITY` | `AUTOINCREMENT` or `IDENTITY` | Use in column definition: `col INT AUTOINCREMENT` |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | Store GUIDs as strings; generate with `UUID_STRING()` |
| `XML` | `VARIANT` | Parse with Snowflake semi-structured functions |
| `JSON` (IQ) | `VARIANT` | Native Snowflake semi-structured type |

---

## 2. Function Translations

### Date/Time Functions

| Sybase Function | Snowflake Equivalent | Example |
|----------------|---------------------|---------|
| `GETDATE()` | `CURRENT_TIMESTAMP()` | `SELECT CURRENT_TIMESTAMP()` |
| `DATEPART(part, date)` | `DATE_PART('part', date)` | `DATEPART(YEAR, col)` → `DATE_PART('YEAR', col)` |
| `DATENAME(part, date)` | See mapping below | Depends on the date part requested |
| `DATEDIFF(part, start, end)` | `DATEDIFF('part', start, end)` | Arg order is the same; quote the part name |
| `DATEADD(part, num, date)` | `DATEADD('part', num, date)` | Arg order is the same; quote the part name |
| `CONVERT(type, expr, style)` | See Convert section below | Depends on target type and style code |
| `YEAR(date)` | `YEAR(date)` | Direct mapping |
| `MONTH(date)` | `MONTH(date)` | Direct mapping |
| `DAY(date)` | `DAY(date)` or `DAYOFMONTH(date)` | Direct mapping |

#### DATENAME Mapping

| Sybase | Snowflake |
|--------|-----------|
| `DATENAME(WEEKDAY, d)` | `DAYNAME(d)` |
| `DATENAME(MONTH, d)` | `MONTHNAME(d)` |
| `DATENAME(YEAR, d)` | `CAST(YEAR(d) AS VARCHAR)` |
| `DATENAME(QUARTER, d)` | `CAST(QUARTER(d) AS VARCHAR)` |

#### CONVERT Function Mapping

The Sybase `CONVERT(target_type, expression [, style])` function is the most complex translation. Map based on the target type:

| Sybase CONVERT Pattern | Snowflake Equivalent | Notes |
|------------------------|---------------------|-------|
| `CONVERT(VARCHAR, date_col, 112)` | `TO_CHAR(date_col, 'YYYYMMDD')` | Style 112 = YYYYMMDD |
| `CONVERT(VARCHAR, date_col, 101)` | `TO_CHAR(date_col, 'MM/DD/YYYY')` | Style 101 = US date |
| `CONVERT(VARCHAR, date_col, 103)` | `TO_CHAR(date_col, 'DD/MM/YYYY')` | Style 103 = British |
| `CONVERT(VARCHAR, date_col, 120)` | `TO_CHAR(date_col, 'YYYY-MM-DD HH24:MI:SS')` | Style 120 = ODBC canonical |
| `CONVERT(VARCHAR, date_col, 23)` | `TO_CHAR(date_col, 'YYYY-MM-DD')` | Style 23 = ISO |
| `CONVERT(INT, expr)` | `CAST(expr AS INT)` or `TO_NUMBER(expr)` | No style = simple cast |
| `CONVERT(DATETIME, str)` | `TO_TIMESTAMP_NTZ(str)` | Parse string to timestamp |
| `CONVERT(DATETIME, str, 112)` | `TO_TIMESTAMP_NTZ(str, 'YYYYMMDD')` | With format code |
| `CONVERT(CHAR(8), date, 112)` | `TO_CHAR(date, 'YYYYMMDD')` | Common date-key pattern |
| `CONVERT(INT, CONVERT(CHAR(8), date, 112))` | `TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'))` | Integer date key pattern |
| `CONVERT(FLOAT, expr)` | `CAST(expr AS FLOAT)` | Simple cast |
| `CONVERT(NUMERIC(p,s), expr)` | `CAST(expr AS NUMBER(p,s))` | Preserve precision |

### String Functions

| Sybase Function | Snowflake Equivalent | Notes |
|----------------|---------------------|---------|
| `ISNULL(expr, replacement)` | `COALESCE(expr, replacement)` or `IFNULL(expr, replacement)` | Prefer COALESCE for multi-arg |
| `LEN(str)` | `LENGTH(str)` | Sybase LEN excludes trailing spaces; Snowflake LENGTH includes them. Use `LENGTH(RTRIM(str))` for exact match |
| `CHARINDEX(substr, str)` | `CHARINDEX(substr, str)` or `POSITION(substr IN str)` | Direct mapping available |
| `PATINDEX(pattern, str)` | `REGEXP_INSTR(str, regex)` | Convert SQL wildcards to regex: `%` → `.*`, `_` → `.` |
| `STUFF(str, start, len, new)` | `INSERT(str, start, len, new)` | Direct equivalent |
| `REPLICATE(str, n)` | `REPEAT(str, n)` | Direct equivalent |
| `LTRIM(str)` | `LTRIM(str)` | Direct mapping |
| `RTRIM(str)` | `RTRIM(str)` | Direct mapping |
| `LEFT(str, n)` | `LEFT(str, n)` | Direct mapping |
| `RIGHT(str, n)` | `RIGHT(str, n)` | Direct mapping |
| `UPPER(str)` | `UPPER(str)` | Direct mapping |
| `LOWER(str)` | `LOWER(str)` | Direct mapping |
| `SUBSTRING(str, start, len)` | `SUBSTR(str, start, len)` | Snowflake uses SUBSTR |
| `STR(num, len, dec)` | `TO_CHAR(num, format)` | Build format string from len/dec |
| `SPACE(n)` | `REPEAT(' ', n)` | No direct SPACE function |
| `REVERSE(str)` | `REVERSE(str)` | Direct mapping |
| `ASCII(char)` | `ASCII(char)` | Direct mapping |
| `CHAR(code)` | `CHR(code)` | Snowflake uses CHR |

### Aggregate and Math Functions

| Sybase Function | Snowflake Equivalent | Notes |
|----------------|---------------------|---------|
| `COUNT_BIG(*)` | `COUNT(*)` | Snowflake COUNT returns BIGINT natively |
| `SQUARE(n)` | `POWER(n, 2)` | No direct SQUARE function |
| `RAND()` | `RANDOM()` or `UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())` | Different semantics |
| `NEWID()` | `UUID_STRING()` | Generate UUID |
| `SIGN(n)` | `SIGN(n)` | Direct mapping |
| `ABS(n)` | `ABS(n)` | Direct mapping |
| `CEILING(n)` | `CEIL(n)` | Snowflake uses CEIL |
| `FLOOR(n)` | `FLOOR(n)` | Direct mapping |
| `ROUND(n, d)` | `ROUND(n, d)` | Direct mapping |

### Null Handling

| Sybase Function | Snowflake Equivalent | Notes |
|----------------|---------------------|---------|
| `ISNULL(a, b)` | `COALESCE(a, b)` | COALESCE supports multiple arguments |
| `NULLIF(a, b)` | `NULLIF(a, b)` | Direct mapping |
| `COALESCE(a, b, c)` | `COALESCE(a, b, c)` | Direct mapping |

---

## 3. Pattern Conversions

### IF EXISTS / DROP / CREATE Pattern

**Sybase:**
```sql
IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'my_table' AND type = 'U')
    DROP TABLE my_table
GO
CREATE TABLE my_table (
    col1 INT,
    col2 VARCHAR(100)
)
GO
```

**Snowflake:**
```sql
CREATE OR REPLACE TABLE my_table (
    col1 INT,
    col2 VARCHAR(100)
);
```

### IDENTITY Column

**Sybase:**
```sql
CREATE TABLE my_table (
    id NUMERIC(10,0) IDENTITY,
    name VARCHAR(100)
)
```

**Snowflake:**
```sql
CREATE OR REPLACE TABLE my_table (
    id NUMBER(10,0) AUTOINCREMENT START 1 INCREMENT 1,
    name VARCHAR(100)
);
```

### Variable Declaration and Assignment

**Sybase:**
```sql
DECLARE @batch_date DATETIME
DECLARE @row_count INT
SELECT @batch_date = GETDATE()
SELECT @row_count = COUNT(*) FROM my_table
```

**Snowflake (Scripting block):**
```sql
DECLARE
    batch_date TIMESTAMP_NTZ;
    row_count INT;
BEGIN
    batch_date := CURRENT_TIMESTAMP();
    SELECT COUNT(*) INTO :row_count FROM my_table;
END;
```

### WHILE Loop

**Sybase:**
```sql
DECLARE @year INT
SELECT @year = 2000
WHILE @year <= 2030
BEGIN
    INSERT INTO dim_date (year_num) VALUES (@year)
    SELECT @year = @year + 1
END
```

**Snowflake (Scripting block):**
```sql
DECLARE
    year_val INT := 2000;
BEGIN
    WHILE (year_val <= 2030) DO
        INSERT INTO dim_date (year_num) VALUES (:year_val);
        year_val := year_val + 1;
    END WHILE;
END;
```

### SCD Type 1 (UPDATE + INSERT)

**Sybase:**
```sql
UPDATE dim_product SET
    product_name = s.product_name,
    category = s.category,
    updated_date = GETDATE()
FROM dim_product d
INNER JOIN stg_products s ON d.product_code = s.product_code
WHERE ISNULL(d.product_name,'') <> ISNULL(s.product_name,'')
   OR ISNULL(d.category,'') <> ISNULL(s.category,'')

INSERT INTO dim_product (product_code, product_name, category, created_date)
SELECT s.product_code, s.product_name, s.category, GETDATE()
FROM stg_products s
WHERE NOT EXISTS (SELECT 1 FROM dim_product d WHERE d.product_code = s.product_code)
```

**Snowflake:**
```sql
MERGE INTO dim_product AS d
USING stg_products AS s
ON d.product_code = s.product_code
WHEN MATCHED AND (
    COALESCE(d.product_name, '') <> COALESCE(s.product_name, '')
    OR COALESCE(d.category, '') <> COALESCE(s.category, '')
) THEN UPDATE SET
    product_name = s.product_name,
    category = s.category,
    updated_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (product_code, product_name, category, created_date)
VALUES (s.product_code, s.product_name, s.category, CURRENT_TIMESTAMP());
```

### SCD Type 2 (Expire + Insert)

**Sybase:**
```sql
-- Expire changed records
UPDATE dim_customer SET
    scd_end_date = GETDATE(),
    scd_current_flag = 'N'
FROM dim_customer d
INNER JOIN stg_customers s ON d.customer_id = s.customer_id
WHERE d.scd_current_flag = 'Y'
  AND (ISNULL(d.name,'') <> ISNULL(s.name,''))

-- Insert new version
INSERT INTO dim_customer (customer_id, name, scd_start_date, scd_end_date, scd_current_flag)
SELECT s.customer_id, s.name, GETDATE(), '9999-12-31', 'Y'
FROM stg_customers s
INNER JOIN dim_customer d ON d.customer_id = s.customer_id
WHERE d.scd_current_flag = 'N'
  AND d.scd_end_date = CONVERT(DATETIME, CONVERT(CHAR(10), GETDATE(), 120))
```

**Snowflake:**
```sql
-- Step 1: Expire changed records
UPDATE dim_customer SET
    scd_end_date = CURRENT_TIMESTAMP(),
    scd_current_flag = 'N'
FROM stg_customers s
WHERE dim_customer.customer_id = s.customer_id
  AND dim_customer.scd_current_flag = 'Y'
  AND (COALESCE(dim_customer.name, '') <> COALESCE(s.name, ''));

-- Step 2: Insert new version for changed records
INSERT INTO dim_customer (customer_id, name, scd_start_date, scd_end_date, scd_current_flag)
SELECT s.customer_id, s.name, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, 'Y'
FROM stg_customers s
INNER JOIN dim_customer d ON d.customer_id = s.customer_id
WHERE d.scd_current_flag = 'N'
  AND d.scd_end_date::DATE = CURRENT_DATE();

-- Step 3: Insert entirely new records
INSERT INTO dim_customer (customer_id, name, scd_start_date, scd_end_date, scd_current_flag)
SELECT s.customer_id, s.name, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP_NTZ, 'Y'
FROM stg_customers s
WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id);
```

### SELECT INTO Temp Table

**Sybase:**
```sql
SELECT col1, col2
INTO #temp_results
FROM my_table
WHERE condition = 1
```

**Snowflake:**
```sql
CREATE OR REPLACE TEMPORARY TABLE temp_results AS
SELECT col1, col2
FROM my_table
WHERE condition = 1;
```

### Linked Server / Cross-Database Reference

**Sybase:**
```sql
SELECT * FROM OperationalDB..dbo.customers
SELECT * FROM LinkedServer.RemoteDB.dbo.orders
```

**Snowflake:**
```sql
-- Same-account cross-database
SELECT * FROM OPERATIONAL_DB.DBO.CUSTOMERS;
-- Remote source: must be pre-loaded via ingestion pipeline
SELECT * FROM RAW_DB.PUBLIC.ORDERS;
```

---

## 4. System Object Replacements

| Sybase System Object | Snowflake Equivalent |
|---------------------|---------------------|
| `sysobjects` (type='U') | `INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'` |
| `sysobjects` (type='V') | `INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW'` |
| `sysobjects` (type='P') | `INFORMATION_SCHEMA.PROCEDURES` |
| `syscolumns` | `INFORMATION_SCHEMA.COLUMNS` |
| `sysindexes` | `INFORMATION_SCHEMA.TABLE_STORAGE_METRICS` or `SHOW INDEXES` |
| `sp_help 'table'` | `DESCRIBE TABLE table_name` or `SHOW COLUMNS IN table_name` |
| `sp_helpindex 'table'` | `SHOW INDEXES IN table_name` |
| `sp_depends 'object'` | `GET_OBJECT_REFERENCES()` or `ACCESS_HISTORY` view |
| `@@ROWCOUNT` | `SQLROWCOUNT` (in Snowflake Scripting) |
| `@@ERROR` | `SQLCODE` (in Snowflake Scripting) |
| `@@IDENTITY` | Use `RESULT_SCAN(LAST_QUERY_ID())` or sequences |
| `@@SERVERNAME` | `CURRENT_ACCOUNT()` |
| `@@VERSION` | `CURRENT_VERSION()` |

---

## 5. Batch and Control Flow

| Sybase Construct | Snowflake Equivalent | Notes |
|-----------------|---------------------|-------|
| `GO` | Remove entirely | Snowflake uses `;` as statement terminator |
| `SET NOCOUNT ON` | Remove entirely | No equivalent needed; Snowflake does not return row counts by default |
| `SET ANSI_NULLS ON` | Remove entirely | Snowflake always uses ANSI NULL behavior |
| `SET QUOTED_IDENTIFIER ON` | Remove entirely | Snowflake always uses double-quote identifier behavior |
| `PRINT 'message'` | `SYSTEM$LOG('INFO', 'message')` | Or `SYSTEM$LOG_TRACE('message')` in a procedure |
| `RAISERROR('msg', 16, 1)` | Snowflake Scripting: `RAISE EXPRESSION_ERROR` | Use `RAISE` with custom exception in scripting blocks |
| `BEGIN TRANSACTION` | `BEGIN TRANSACTION` | Direct mapping (auto-commit must be off) |
| `COMMIT TRANSACTION` | `COMMIT` | Direct mapping |
| `ROLLBACK TRANSACTION` | `ROLLBACK` | Direct mapping |
| `BEGIN TRY...END TRY BEGIN CATCH...END CATCH` | `BEGIN ... EXCEPTION WHEN OTHER THEN ... END` | Snowflake Scripting exception handling |
| `EXEC` / `EXECUTE` | `CALL` | For stored procedure invocation |
| `IF...ELSE` | `IF...ELSEIF...ELSE...END IF` | Snowflake Scripting requires END IF |

---

## 6. String and Date Format Codes

### Sybase CONVERT Style Codes to Snowflake Format Strings

| Style | Sybase Output Format | Snowflake TO_CHAR Format |
|-------|---------------------|-------------------------|
| 0 / 100 | `Mon DD YYYY HH:MIAM` | `'MON DD YYYY HH12:MIAM'` |
| 1 / 101 | `MM/DD/YYYY` | `'MM/DD/YYYY'` |
| 2 / 102 | `YYYY.MM.DD` | `'YYYY.MM.DD'` |
| 3 / 103 | `DD/MM/YYYY` | `'DD/MM/YYYY'` |
| 4 / 104 | `DD.MM.YYYY` | `'DD.MM.YYYY'` |
| 5 / 105 | `DD-MM-YYYY` | `'DD-MM-YYYY'` |
| 6 / 106 | `DD Mon YYYY` | `'DD MON YYYY'` |
| 7 / 107 | `Mon DD, YYYY` | `'MON DD, YYYY'` |
| 8 / 108 | `HH:MI:SS` | `'HH24:MI:SS'` |
| 10 / 110 | `MM-DD-YYYY` | `'MM-DD-YYYY'` |
| 11 / 111 | `YYYY/MM/DD` | `'YYYY/MM/DD'` |
| 12 / 112 | `YYYYMMDD` | `'YYYYMMDD'` |
| 14 / 114 | `HH:MI:SS:MMM` | `'HH24:MI:SS.FF3'` |
| 20 / 120 | `YYYY-MM-DD HH:MI:SS` | `'YYYY-MM-DD HH24:MI:SS'` |
| 21 / 121 | `YYYY-MM-DD HH:MI:SS.MMM` | `'YYYY-MM-DD HH24:MI:SS.FF3'` |
| 23 | `YYYY-MM-DD` | `'YYYY-MM-DD'` |
| 126 | `YYYY-MM-DDTHH:MI:SS.MMM` | `'YYYY-MM-DD"T"HH24:MI:SS.FF3'` |
