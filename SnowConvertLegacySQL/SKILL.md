---
name: SnowConvertLegacySQL
description: "**[REQUIRED]** Convert legacy SQL (SQL Server, Oracle, Teradata) to Snowflake-native SQL using SnowConvert patterns, then chain to ConvertLegacySQL for dbt transformation. Use when user provides legacy stored procedures, DDL, or complex SQL for migration."
---

# SnowConvert Legacy SQL Skill

## Overview
This skill performs **Step 1** of the legacy SQL migration pipeline:
1. **SnowConvertLegacySQL** (this skill) → Syntax conversion to Snowflake SQL
2. **ConvertLegacySQL** → dbt model generation and optimization

## When to Invoke
- User provides SQL Server, Oracle, or Teradata SQL code
- User asks to "convert", "migrate", or "transform" legacy SQL
- User mentions "SnowConvert" or "Snowflake migration"

## Step 1: Detect Source Platform

Identify the source dialect from syntax patterns:

| Platform | Indicators |
|----------|------------|
| **SQL Server** | `[brackets]`, `#temp`, `##global_temp`, `GETDATE()`, `@@IDENTITY`, `SET NOCOUNT ON`, `TOP N`, `ISNULL()` |
| **Oracle** | `NVL()`, `SYSDATE`, `ROWNUM`, `DECODE()`, `(+)` outer join, `PL/SQL`, `DUAL`, `CONNECT BY` |
| **Teradata** | `QUALIFY`, `SEL`, `.database.table`, `CASESPECIFIC`, `TITLE`, `COMPRESS` |

## Step 2: Apply SnowConvert Transformation Rules

### Data Types
| Source | Snowflake |
|--------|-----------|
| `VARCHAR(MAX)`, `NVARCHAR(MAX)` | `VARCHAR(16777216)` |
| `DATETIME`, `DATETIME2` | `TIMESTAMP_NTZ` |
| `SMALLDATETIME` | `TIMESTAMP_NTZ` |
| `MONEY`, `SMALLMONEY` | `NUMBER(19,4)` |
| `BIT` | `BOOLEAN` |
| `IMAGE`, `VARBINARY(MAX)` | `BINARY` |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` |
| `NUMBER(*)` (Oracle) | `NUMBER(38,0)` |
| `CLOB`, `BLOB` | `VARCHAR`, `BINARY` |

### Functions
| Source | Snowflake |
|--------|-----------|
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `SYSDATE` | `CURRENT_DATE()` or `CURRENT_TIMESTAMP()` |
| `ISNULL(a,b)` | `NVL(a,b)` or `COALESCE(a,b)` |
| `CHARINDEX(a,b)` | `POSITION(a IN b)` or `CHARINDEX(a,b)` |
| `LEN()` | `LENGTH()` |
| `DATEADD(unit,n,date)` | `DATEADD(unit, n, date)` ✅ |
| `DATEDIFF(unit,a,b)` | `DATEDIFF(unit, a, b)` ✅ |
| `CONVERT(type, val)` | `CAST(val AS type)` or `TO_*()` |
| `CAST(x AS VARCHAR)` | `CAST(x AS VARCHAR)` ✅ |
| `@@IDENTITY` | `<sequence>.CURRVAL` or use AUTOINCREMENT |
| `@@ROWCOUNT` | Use `RESULT_SCAN()` |
| `DECODE()` | `CASE WHEN` or `DECODE()` ✅ |
| `ROWNUM` | `ROW_NUMBER() OVER()` with `QUALIFY` |
| `CONNECT BY` | Recursive CTE |

### Temp Tables
| Source | Snowflake |
|--------|-----------|
| `#local_temp` | `CREATE TEMPORARY TABLE` (session-scoped) |
| `##global_temp` | `CREATE TEMPORARY TABLE` or permanent table |
| `DECLARE @table TABLE()` | `CREATE TEMPORARY TABLE` |

### Control Flow & Procedures
| Source | Snowflake |
|--------|-----------|
| `SET NOCOUNT ON` | Remove (not needed) |
| `BEGIN...END` | `BEGIN...END` ✅ |
| `IF...ELSE` | `IF...ELSEIF...ELSE...END IF` |
| `WHILE` | `WHILE...DO...END WHILE` or `LOOP` |
| `TRY...CATCH` | `EXCEPTION WHEN OTHER THEN` |
| `RAISERROR` | `RAISE` or custom exception |
| `PRINT` | Remove or use logging table |
| `EXEC sp_executesql` | `EXECUTE IMMEDIATE` |

### UPDATE/DELETE with JOIN
```sql
-- SQL Server
UPDATE t SET t.col = s.val FROM target t JOIN source s ON t.id = s.id

-- Snowflake
UPDATE target t SET t.col = s.val FROM source s WHERE t.id = s.id
```

### SELECT INTO / INSERT
```sql
-- SQL Server
SELECT * INTO #NewTable FROM Source

-- Snowflake  
CREATE TEMPORARY TABLE NewTable AS SELECT * FROM Source
```

## Step 3: Generate Assessment Report

Produce a markdown table:

```markdown
| # | Issue | Severity | Original | Converted | Notes |
|---|-------|----------|----------|-----------|-------|
| 1 | Temp table | 🟡 | `#Sales` | `TEMPORARY TABLE Sales` | Auto-drops at session end |
| 2 | Function | 🟢 | `GETDATE()` | `CURRENT_TIMESTAMP()` | Direct replacement |
| 3 | UPDATE JOIN | 🔴 | `UPDATE...FROM...JOIN` | `UPDATE...FROM...WHERE` | Syntax change required |
```

**Severity Legend:**
- 🔴 **High**: Requires manual review or logic change
- 🟡 **Medium**: Syntax differs but straightforward conversion
- 🟢 **Low**: Minor or compatible change

## Step 4: Output Converted Snowflake SQL

Provide the complete converted SQL with:
- Comments marking any `-- REVIEW:` areas needing manual verification
- Proper Snowflake syntax and formatting
- Preserved business logic

## Step 5: Chain to ConvertLegacySQL

After syntax conversion is complete, prompt:

```
✅ SnowConvert syntax transformation complete.

**Next Step**: Would you like me to invoke `ConvertLegacySQL` to:
- Generate dbt models from this Snowflake SQL
- Create staging/mart layer structure  
- Add tests and documentation
- Optimize for incremental loads

Reply "yes" or "convert to dbt" to continue.
```

When user confirms, invoke the `ConvertLegacySQL` skill with the converted Snowflake SQL.

---

## Example Workflow

**User Input:**
```sql
CREATE PROCEDURE dbo.GetMonthlySales @Month INT AS
SELECT TOP 10 * FROM #TempSales WHERE MONTH(SaleDate) = @Month
```

**SnowConvertLegacySQL Output:**
1. Assessment table showing 3 issues
2. Converted Snowflake procedure
3. Prompt to chain to ConvertLegacySQL

**ConvertLegacySQL Output:**
1. dbt staging model
2. dbt mart model  
3. schema.yml with tests
4. Incremental strategy recommendation

