# SKILL CHANGELOG — dbt-model-optimizer

## [0.2.0] - 2026-04-08

### Added
- Persistent file-based optimization report per model (`optimization_report_{model_name}.md`)
- New input parameter `report_dir` to collect reports in a shared directory
- Report output settings in config.yml (`file_name_pattern`, `report_dir`, `overwrite_existing`, `include_timestamp`, `include_model_snapshot`)
- Report file header includes model name, detected dialect, timestamp, Snowflake connection status
- Step 10 now updates the report file with post-optimization sections (changes applied, validation, remaining recommendations)
- Inline summary now references the report file path

### Changed
- Output Specification updated: report file is now the primary deliverable, inline summary is secondary
- Step 6 writes the initial report file (pre-optimization findings)
- Step 10 appends post-optimization results to the same report file

## [0.1.0] - 2026-04-07

### Initial release (draft)
- 10-step orchestrator SKILL.md with YAML frontmatter
- Sub-agent A: static-analyzer (D1-D4 analysis, no Snowflake required)
- Sub-agent B: live-profiler (D5-D6 analysis, requires Snowflake)
- Reference documents:
  - dialect-patterns.md: Oracle, Teradata, SQL Server, Informatica patterns
  - optimization-rules.md: master rule catalog (69 rules across 6 dimensions)
  - snowflake-native.md: QUALIFY, FLATTEN, SPLIT_TO_TABLE, MATCH_RECOGNIZE, Dynamic Table
  - dbt-conventions.md: config, CTE, naming, dependency, hook, audit column rules
- config.yml: thresholds, dimension toggles, dialect detection patterns
- Example 01: dialect cleanup (Oracle NVL/DECODE/SYSDATE in Teradata-migrated model)
- Example 02: CTE rewrite (unused CTEs, missing QUALIFY, missing tests)
- evals.json: 6 test cases across all dimensions
- Absorbed patterns from: dbt-sql-optimizer (Agent 7), SnowConvertLegacySQL,
  assessment_framework.yaml (24 rules), INFA2DBT prescription patterns

### Sources absorbed
- `dbt-sql-optimizer` (INFA2DBT Agent 7): CTE pruning (Pattern A), predicate
  pushdown (Pattern B), CTE consolidation (Pattern C)
- `SnowConvertLegacySQL`: 30+ dialect conversion mappings across data types,
  functions, control flow, temp tables
- `assessment_framework.yaml`: 24 rule-based checks, 7 semantic criteria,
  12 guardrails, 8 Snowflake function mappings
- `OptimizeDbtModel` (ConvertLegacySQL Step 4): guided Query Profile approach
