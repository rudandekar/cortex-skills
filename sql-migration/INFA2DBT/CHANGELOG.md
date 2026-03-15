# INFA2DBT Accelerator Changelog

All notable changes to the INFA2DBT Accelerator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-03-15

### Added
- **RAG-Enhanced Conversion** via Cortex Search service
  - `INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS` table for labelled examples
  - `INFA2DBT_CORPUS_SEARCH` Cortex Search service for semantic retrieval
  - Agent 2 queries corpus before generating DBT code
- **Persistent State Store** in Snowflake tables
  - `PIPELINE_STATE` - Track pipeline runs, status, phases
  - `MODEL_REGISTRY` - Store all converted DBT models with SQL content
  - `FIDELITY_SCORES` - Track conversion quality metrics per model
- Resume-from-failure with Snowflake-backed state
- Model registry with fidelity tracking and audit trail
- 12 seeded transformation examples in corpus
- Monitoring queries for pipeline progress and quality metrics

### Security
- Fixed SQL injection vulnerability in Cortex Search queries (parameterized JSON)
- Fixed JSON injection in search_corpus() payload construction
- Added escape_dollar_quotes() to prevent $$ literal injection
- Connection pooling via context manager (single connection per batch)

### Changed
- Agent 2 upgraded to v2.0 with corpus search integration
- Orchestrator skill upgraded to v2.0 with persistent state
- State management moved from file-based JSON to Snowflake tables
- Truncation limit increased to 500 chars with warning comments
- Lookup CTE uses explicit column selection instead of invalid EXCLUDE syntax
- Column list truncation uses block comments instead of inline comments

### Fixed
- ROI weight inconsistency between Step 5 and Reference Tables
- Gate 1 risk level corrected to HIGH (was MEDIUM)
- Duplicate Step 9 in converter SKILL.md (now Step 9 and Step 10)
- Warehouse name standardized to INFA2DBT_WH across all docs
- Version references updated from v1.1 to v2.0.0
- Hardcoded dev path removed from agent1_parser.py (now uses argparse CLI)

### Migration Notes
- Requires creating `INFA2DBT_DB.PIPELINE` schema
- See `MIGRATION.md` for setup instructions
- See `ROLES_AND_PRIVILEGES.md` for required permissions

---

## [1.1.0] - 2026-03-15

### Added
- Initial release of INFA2DBT Accelerator as Cortex Code Skills
- 7 specialized agent skills:
  - `infa-xml-parser` (Agent 1): XML extraction and decomposition
  - `infa-to-dbt-converter` (Agent 2): DBT code generation
  - `conversion-fidelity-scorer` (Agent 3): Semantic validation
  - `dbt-validation-critique` (Agent 4): Compliance validation
  - `phase1-handoff` (Agent 5): Checkpoint management
  - `roi-subgraph-scorer` (Agent 6): ROI analysis
  - `dbt-sql-optimizer` (Agent 7A/B): Performance optimization
- Main orchestrator skill: `infa2dbt-accelerator`
- 6 HITL gates with sign-off packages
- Transformation type map for 11 core Informatica transformations
- Coding guidelines template
- 3-tier retry/quarantine routing
- Unit test generation per transformation type

### Reference Documents
- `transformation-type-map.md` - Complete transformation mapping
- `coding-guidelines-template.md` - Customizable coding standards

### Supported Transformation Types
- Source Qualifier (with/without SQL override)
- Expression (with function translation)
- Aggregator (all aggregate functions)
- Lookup (connected and unconnected)
- Joiner (all join types)
- Filter
- Router (multi-group)
- Normalizer (LATERAL FLATTEN)
- Rank (window functions)
- Update Strategy (incremental)
- Sequence Generator

## [Planned]

### [1.2.0] - TBD
- Support for Stored Procedure transformations via Snowpark
- XML Parser transformation support
- Parallel workflow processing
- Enhanced corpus management

### [1.3.0] - TBD
- Talend job converter integration
- SSIS package converter integration
- Cross-platform migration orchestration
