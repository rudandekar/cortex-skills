# dbt Compile Error Reference

Lookup table for common `dbt compile` errors, root causes, and fixes.

## Jinja Errors

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Compilation Error: unexpected '}'` | Missing `{%` opening tag or extra `}` | Check Jinja block delimiters: `{% %}`, `{{ }}`, `{# #}` |
| `Compilation Error: expected token 'end of statement block', got '='` | Using `=` instead of `==` in Jinja `{% if %}` | Replace `=` with `==` in conditionals |
| `UndefinedError: 'var_name' is undefined` | Referencing a Jinja variable that doesn't exist | Define variable with `{% set %}` or use `var()` with default |
| `TemplateSyntaxError` | Malformed Jinja template | Check for unclosed tags, mismatched delimiters |

## ref() Errors

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Compilation Error: model 'X' was not found` | Model doesn't exist in project | Create the model or fix the name typo |
| `Compilation Error: ref('X') ... disabled` | Referenced model is disabled in schema.yml | Enable the model or remove the reference |
| `Node is not enabled` | Model excluded by selector or disabled | Check `enabled` config and selector rules |

### Forward Reference vs. Missing Model

During incremental migration (e.g., INFA2DBT), a `missing_ref` error may be a
**forward reference** — the model exists in the source system but hasn't been
converted yet. To distinguish:

- **Forward reference:** Model name appears in the Informatica workflow inventory
  but not yet in the dbt project. Expected during phased migration. Log as
  WARNING, do not fail.
- **Missing model:** Model name doesn't appear in any inventory. Likely a typo
  or architectural gap. Log as ERROR.

## source() Errors

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Compilation Error: source 'X' was not found` | Source not declared in any `sources.yml` | Add source definition to `models/sources.yml` |
| `Compilation Error: source 'X' table 'Y' was not found` | Table name not listed under the source | Add table entry under the source definition |
| `Source is disabled` | Source marked as `enabled: false` | Enable source or remove reference |

### Suggested sources.yml Template

```yaml
version: 2
sources:
  - name: raw
    database: "{{ env_var('DBT_SOURCE_DATABASE', 'RAW') }}"
    schema: "{{ env_var('DBT_SOURCE_SCHEMA', 'PUBLIC') }}"
    tables:
      - name: table_name
        description: "Source table description"
```

## YAML Parse Errors

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Parsing Error: invalid syntax in YAML` | Indentation error or invalid YAML | Validate YAML with a linter; check indentation (2 spaces) |
| `Parsing Error: duplicate key` | Same key appears twice in YAML file | Remove duplicate key |
| `Parsing Error: expected a mapping` | YAML structure doesn't match dbt schema | Check dbt schema docs for expected structure |

## Circular Reference Errors

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Found a cycle: model.A -> model.B -> model.A` | Two or more models reference each other | Refactor to break the cycle: extract shared logic into a base model or use sources instead of refs for one direction |

## Snowflake EXPLAIN Errors

These occur during Step 5 (SQL validation on Snowflake) after a successful compile:

| Error Pattern | Root Cause | Fix |
|---------------|-----------|-----|
| `Object 'X' does not exist or not authorized` | Table/view not found in Snowflake | Check: (1) table exists, (2) database/schema context is correct, (3) role has SELECT privilege |
| `invalid identifier 'COLUMN_NAME'` | Column doesn't exist in the referenced table | Check column name spelling; compare against source schema |
| `SQL compilation error: syntax error` | Valid Jinja but invalid Snowflake SQL | Review the compiled SQL for Snowflake-specific syntax issues |
| `Insufficient privileges` | Role lacks required permissions | Grant SELECT on the source tables to the dbt role |
| `Warehouse 'X' does not exist` | No active warehouse for EXPLAIN | Set warehouse in profiles.yml or session |

## Informatica Artifact Errors

Artifacts from Informatica that survive into compiled SQL and cause Snowflake errors:

| Artifact | SQL Impact | Fix |
|----------|-----------|-----|
| `SETVARIABLE($$VAR, value)` | Not valid SQL — syntax error | Convert to dbt `var()` or Snowflake session variable |
| `$$VARIABLE_NAME` | Informatica parameter — unresolved | Replace with `{{ var('variable_name') }}` or literal value |
| `IIF(condition, true, false)` | Informatica function — not Snowflake | Replace with `CASE WHEN ... THEN ... ELSE ... END` or `IFF()` |
| `DECODE(value, ...)` | May work but non-standard | Convert to `CASE WHEN` or Snowflake `DECODE` (check arity) |
| `TO_DATE(string, 'MM/DD/YYYY')` | Format may differ | Use Snowflake date format: `TO_DATE(string, 'MM/DD/YYYY')` (verify format tokens) |
| `LTRIM/RTRIM(string, chars)` | Snowflake LTRIM/RTRIM have different char-set behavior | Test behavior; may need `TRIM(string, chars)` |
