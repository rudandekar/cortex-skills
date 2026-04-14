# Example 01: Basic Compile Check

## Scenario

A dbt model converted from Informatica PowerCenter workflow
`wf_m_COMPTIME_Current_Pay_Period` needs compile validation.
The model uses `{{ source() }}` references, CTE patterns, and contains
Informatica artifacts (`SETVARIABLE`, `v_` prefixed variables) that should
be flagged as warnings.

## Input

- `input/mart_comp_time_daily_tbl.sql` — Converted dbt model with Jinja templates
- `input/dbt_project.yml` — Minimal dbt project configuration
- `input/sources.yml` — Source definitions for raw tables

## Expected Behavior

### Step 1: Environment validation
- dbt is installed
- `dbt_project.yml` found at project path

### Step 3: Execute dbt compile
- Exit code 0 (compile succeeds)
- Jinja `{{ source('raw', 'u0287d01') }}` resolves to `RAW.PUBLIC.U0287D01`
- Jinja `{{ source('raw', 'pay_period') }}` resolves to `RAW.PUBLIC.PAY_PERIOD`

### Step 4: Compile result analysis
- 4 CTEs detected: `source_u0287d01`, `source_pay_period`, `transformed_exp_build_pay_period`, `transformed_exp_final`, `final`
- Informatica artifacts flagged:
  - Line 51: `SETVARIABLE($$MAP_PP_YEAR_NUM, v_PAY_PERIOD)` — ERROR (not valid Snowflake SQL)
  - Line 52: `SETVARIABLE($$MAP_PP_END_YEAR, PP_END_YEAR)` — ERROR
  - Line 53: `SETVARIABLE($$MAP_PP_NUM, PP_NUM)` — ERROR
  - Line 50: `v_PP_NUM` — WARNING (unconverted Informatica variable)
- TODO comments: 1 found (line 27)
- Final column count: 15

### Step 5: Snowflake validation (if enabled)
- EXPLAIN would fail on `SETVARIABLE` — syntax error expected

## Output

See `output/compile_report.json` for expected structured output.
See `output/compile_summary.md` for expected human-readable summary.
