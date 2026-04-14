# Dialect Conversion Patterns

Reference document for `dbt-model-optimizer` Step 2 (Dimension D1). Consolidated
from SnowConvertLegacySQL, Cisco V6 assessment_framework.yaml function mappings,
and INFA2DBT prescription patterns.

---

## Table of Contents

1. [Oracle Patterns](#oracle-patterns)
2. [Teradata Patterns](#teradata-patterns)
3. [SQL Server Patterns](#sql-server-patterns)
4. [Cross-Dialect Patterns](#cross-dialect-patterns)
5. [Informatica Variable Patterns](#informatica-variable-patterns)

---

## Oracle Patterns

### Functions

| Rule ID | Oracle | Snowflake | Safety | Notes |
|---------|--------|-----------|--------|-------|
| D1-ORA-001 | `NVL(a, b)` | `COALESCE(a, b)` | SAFE | COALESCE handles multiple args; NVL only two |
| D1-ORA-002 | `NVL2(a, b, c)` | `IFF(a IS NOT NULL, b, c)` | SAFE | |
| D1-ORA-003 | `DECODE(col, v1, r1, v2, r2, default)` | `CASE WHEN col = v1 THEN r1 WHEN col = v2 THEN r2 ELSE default END` | SAFE | Snowflake also supports DECODE() natively; use CASE for clarity |
| D1-ORA-004 | `SYSDATE` | `CURRENT_DATE()` | SAFE | Use `CURRENT_TIMESTAMP()` if time component needed |
| D1-ORA-005 | `SYSTIMESTAMP` | `CURRENT_TIMESTAMP()` | SAFE | |
| D1-ORA-006 | `ROWNUM` | `ROW_NUMBER() OVER (ORDER BY ...)` | REVIEW | Requires explicit ORDER BY — user must specify sort column |
| D1-ORA-007 | `TO_CHAR(date, fmt)` | `TO_VARCHAR(date, fmt)` | SAFE | Format string may need adjustment (e.g., `'YYYY-MM-DD'` is compatible) |
| D1-ORA-008 | `SUBSTR(str, pos, len)` | `SUBSTRING(str, pos, len)` | SAFE | Snowflake SUBSTR also works; SUBSTRING is ANSI standard |
| D1-ORA-009 | `INSTR(str, substr)` | `POSITION(substr IN str)` | SAFE | For `INSTR(str, substr, pos, occurrence)` use `REGEXP_INSTR` |
| D1-ORA-010 | `(+)` outer join syntax | ANSI `LEFT/RIGHT JOIN` | REVIEW | Requires understanding join direction |
| D1-ORA-011 | `CONNECT BY ... START WITH` | Recursive CTE | REVIEW | Complex rewrite — must preserve hierarchy |
| D1-ORA-012 | `FROM DUAL` | Remove `FROM DUAL` (Snowflake allows bare SELECT) | SAFE | |
| D1-ORA-013 | `NUMBER(*)` | `NUMBER(38,0)` | SAFE | In DDL/CAST contexts |
| D1-ORA-014 | `CLOB` | `VARCHAR(16777216)` | SAFE | In DDL contexts |
| D1-ORA-015 | `BLOB` | `BINARY` | SAFE | In DDL contexts |

### Control Flow (in stored procedures)

| Rule ID | Oracle | Snowflake | Safety | Notes |
|---------|--------|-----------|--------|-------|
| D1-ORA-020 | `PL/SQL EXCEPTION WHEN` | `EXCEPTION WHEN OTHER THEN` | REVIEW | Error handling restructure |
| D1-ORA-021 | `DBMS_OUTPUT.PUT_LINE` | Remove or log to table | SAFE | No equivalent in Snowflake SQL |

---

## Teradata Patterns

### Syntax

| Rule ID | Teradata | Snowflake | Safety | Notes |
|---------|----------|-----------|--------|-------|
| D1-TD-001 | `SEL ` (as SELECT alias) | `SELECT` | SAFE | Teradata shorthand |
| D1-TD-002 | `BT;` (begin transaction) | Remove | SAFE | Snowflake auto-commits; explicit transactions use `BEGIN` |
| D1-TD-003 | `ET;` (end transaction) | Remove | SAFE | |
| D1-TD-004 | `COLLECT STATISTICS` / `COLLECT STATS` | Remove | SAFE | Snowflake handles micro-partitions automatically |
| D1-TD-005 | `CREATE VOLATILE TABLE` | CTE or `CREATE TEMPORARY TABLE` | REVIEW | Prefer CTE in dbt models; TEMP TABLE if needed in hooks |
| D1-TD-006 | `.database.table` (dot notation) | `DATABASE.SCHEMA.TABLE` | REVIEW | Must map to correct Snowflake namespace |
| D1-TD-007 | `CASESPECIFIC` / `NOT CASESPECIFIC` | Remove (Snowflake is case-insensitive by default) | SAFE | For CS comparison, use `COLLATE` |
| D1-TD-008 | `TITLE 'column title'` | Remove (use column alias or schema.yml description) | SAFE | |
| D1-TD-009 | `COMPRESS` | Remove (Snowflake compresses automatically) | SAFE | |
| D1-TD-010 | `QUALIFY ROW_NUMBER() OVER(...) = 1` | Keep as-is | SAFE | Snowflake natively supports QUALIFY |
| D1-TD-011 | `FORMAT 'YYYY-MM-DD'` | Remove or use `TO_VARCHAR(col, 'YYYY-MM-DD')` | SAFE | |
| D1-TD-012 | `SAMPLE n` | `TABLESAMPLE (n ROWS)` | SAFE | |
| D1-TD-013 | `CHARACTERS` / `GRAPHIC` types | `VARCHAR` | SAFE | In DDL contexts |

### Functions

| Rule ID | Teradata | Snowflake | Safety | Notes |
|---------|----------|-----------|--------|-------|
| D1-TD-020 | `ZEROIFNULL(x)` | `COALESCE(x, 0)` | SAFE | |
| D1-TD-021 | `NULLIFZERO(x)` | `NULLIF(x, 0)` | SAFE | |
| D1-TD-022 | `STRTOK(str, delim, n)` | `SPLIT_PART(str, delim, n)` | SAFE | |
| D1-TD-023 | `INDEX(str, substr)` | `POSITION(substr IN str)` | SAFE | |
| D1-TD-024 | `OREPLACE(str, old, new)` | `REPLACE(str, old, new)` | SAFE | |
| D1-TD-025 | `TRIM(BOTH FROM str)` | `TRIM(str)` | SAFE | |

### UPDATE with FROM (Teradata syntax)

| Rule ID | Teradata | Snowflake | Safety |
|---------|----------|-----------|--------|
| D1-TD-030 | `UPDATE t FROM t, s WHERE t.id = s.id SET t.col = s.val` | `UPDATE t SET t.col = s.val FROM s WHERE t.id = s.id` | REVIEW |

---

## SQL Server Patterns

### Functions

| Rule ID | SQL Server | Snowflake | Safety | Notes |
|---------|------------|-----------|--------|-------|
| D1-SS-001 | `GETDATE()` | `CURRENT_TIMESTAMP()` | SAFE | |
| D1-SS-002 | `ISNULL(a, b)` | `COALESCE(a, b)` | SAFE | |
| D1-SS-003 | `LEN(str)` | `LENGTH(str)` | SAFE | |
| D1-SS-004 | `CHARINDEX(substr, str)` | `POSITION(substr IN str)` | SAFE | Snowflake also supports `CHARINDEX` |
| D1-SS-005 | `CONVERT(type, val)` | `CAST(val AS type)` or `TRY_CAST(val AS type)` | SAFE | Format codes need manual review |
| D1-SS-006 | `@@IDENTITY` | `<sequence>.CURRVAL` or `AUTOINCREMENT` | REVIEW | Depends on identity column setup |
| D1-SS-007 | `@@ROWCOUNT` | `RESULT_SCAN(LAST_QUERY_ID())` | REVIEW | Different semantics |
| D1-SS-008 | `TOP N` (before SELECT columns) | `LIMIT N` (at end of query) | SAFE | |

### Syntax

| Rule ID | SQL Server | Snowflake | Safety | Notes |
|---------|------------|-----------|--------|-------|
| D1-SS-010 | `[bracket_identifiers]` | `"double_quote_identifiers"` | SAFE | Or remove if standard names |
| D1-SS-011 | `#local_temp_table` | `CREATE TEMPORARY TABLE` | REVIEW | Prefer CTE in dbt models |
| D1-SS-012 | `##global_temp_table` | `CREATE TEMPORARY TABLE` | REVIEW | |
| D1-SS-013 | `SET NOCOUNT ON` | Remove | SAFE | Not needed in Snowflake |
| D1-SS-014 | `SELECT * INTO #NewTable FROM ...` | `CREATE TEMPORARY TABLE NewTable AS SELECT ...` | REVIEW | Prefer CTE |
| D1-SS-015 | `DECLARE @table TABLE(...)` | `CREATE TEMPORARY TABLE` | REVIEW | |

### Data Types

| Rule ID | SQL Server | Snowflake | Safety | Notes |
|---------|------------|-----------|--------|-------|
| D1-SS-020 | `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `VARCHAR(16777216)` | SAFE | |
| D1-SS-021 | `DATETIME` / `DATETIME2` | `TIMESTAMP_NTZ` | SAFE | |
| D1-SS-022 | `SMALLDATETIME` | `TIMESTAMP_NTZ` | SAFE | |
| D1-SS-023 | `MONEY` / `SMALLMONEY` | `NUMBER(19,4)` | SAFE | |
| D1-SS-024 | `BIT` | `BOOLEAN` | SAFE | |
| D1-SS-025 | `IMAGE` / `VARBINARY(MAX)` | `BINARY` | SAFE | |
| D1-SS-026 | `UNIQUEIDENTIFIER` | `VARCHAR(36)` | SAFE | |

### Control Flow

| Rule ID | SQL Server | Snowflake | Safety | Notes |
|---------|------------|-----------|--------|-------|
| D1-SS-030 | `IF...ELSE` | `IF...ELSEIF...ELSE...END IF` | SAFE | In procedure context |
| D1-SS-031 | `WHILE` | `WHILE...DO...END WHILE` | SAFE | |
| D1-SS-032 | `TRY...CATCH` | `EXCEPTION WHEN OTHER THEN` | REVIEW | Different error handling model |
| D1-SS-033 | `RAISERROR` | `RAISE` or custom exception | REVIEW | |
| D1-SS-034 | `EXEC sp_executesql` | `EXECUTE IMMEDIATE` | REVIEW | |

### UPDATE with JOIN

| Rule ID | SQL Server | Snowflake | Safety |
|---------|------------|-----------|--------|
| D1-SS-040 | `UPDATE t SET t.col = s.val FROM target t JOIN source s ON t.id = s.id` | `UPDATE target t SET t.col = s.val FROM source s WHERE t.id = s.id` | REVIEW |

---

## Cross-Dialect Patterns

These patterns appear across multiple source dialects:

| Rule ID | Pattern | Snowflake | Safety | Notes |
|---------|---------|-----------|--------|-------|
| D1-XD-001 | `SELECT *` | Enumerate columns explicitly | REVIEW | Best practice but requires column knowledge |
| D1-XD-002 | Implicit type coercion | Add explicit `CAST()` | REVIEW | Snowflake is stricter with types |
| D1-XD-003 | Empty string `''` treated as NULL | Add explicit `NULLIF('', col)` or handle both | REVIEW | Oracle treats `''` as NULL; Snowflake does not |

---

## Informatica Variable Patterns

Residual Informatica variable references that should be replaced:

| Rule ID | Variable Pattern | Replacement | Safety | Notes |
|---------|-----------------|-------------|--------|-------|
| D1-INF-001 | `$$STGDB` | Actual Snowflake schema reference or `var('stg_database')` | SAFE | |
| D1-INF-002 | `$$FINLGLVWDB` | Actual Snowflake schema reference or `var('fin_database')` | SAFE | |
| D1-INF-003 | `$$COMREFVWDB` | Actual Snowflake schema reference or `var('ref_database')` | SAFE | |
| D1-INF-004 | `$PM*` (any `$PM` prefixed var) | dbt `var()` or environment variable | REVIEW | Must determine correct value |
| D1-INF-005 | `$$` (any double-dollar var) | dbt `var()` or remove | REVIEW | Must determine correct value |
