# INFA2DBT Accelerator Changelog

All notable changes to the INFA2DBT Accelerator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - 2026-03-20

### Added
- **PowerCenterParser v3.0** (`agents/infa-xml-parser/scripts/parse_pc_xml.py`) — complete OOP rewrite with gap remediation from corpus scan of 34,503 XMLs
  - `PowerCenterParser` class replaces procedural parsing logic
  - `TYPE_ALIASES` normalization: `Lookup` → `Lookup Procedure`, `Sequence Generator` → `Sequence`
  - `INSTANCE` element extraction with `instance_map` for connector graph accuracy (234,704 instances across corpus)
  - `WORKLET` extraction (3,420 worklets in 1,866 files) — expands worklet-scoped sessions
  - `SHORTCUT` resolution (3,774 cross-folder mapping references)
  - `FLATFILE` source/target definitions (56,739 flat file objects in 15,837 files / 45.9% of corpus)
  - `ASSOCIATED_SOURCE_INSTANCE` for multi-source Source Qualifier joins
  - `MAPPINGVARIABLE` / `WORKFLOWVARIABLE` extraction for `$$param` → dbt `var()` mapping
  - `Command` task type extraction (16,447 instances) for pre_hook/post_hook mapping
  - `TARGETLOADORDER` for intra-mapping target execution ordering
  - Missing transform types: `XML Source Qualifier`, `Input Transformation`, `Output Transformation`, `Mapplet`
  - `CONFIG` / `CONFIGREFERENCE`, `PARTITION`, `SESSIONCOMPONENT` extraction
  - `SCHEDULER` / `SCHEDULEINFO` for improved frequency inference
  - `GROUP`, `SESSTRANSFORMATIONINST` session-level overrides
  - `ERPINFO`, `METADATAEXTENSION`, `FIELDDEPENDENCY` metadata extraction
  - Malformed XML sanitizer with automatic re-parse for the 50 broken files (ampersand/special character escaping)
- **Batch dbt Model Generator** (`agents/infa-to-dbt-converter/scripts/agent2_batch.py`) — programmatic batch converter
  - Tier classification: Tier 1 (simple SQ-only), Tier 2 (2-3 standard transforms), Tier 3 (complex/escalate)
  - Teradata-to-Snowflake SQL dialect conversion (`USER` → `CURRENT_USER()`, `CURRENT_TIMESTAMP(0)` → `CURRENT_TIMESTAMP()`, `NVL` → `COALESCE`, etc.)
  - Post SQL `UPDATE...FROM` → Snowflake `MERGE INTO` conversion with automatic join condition extraction
  - `$$param` → `{{ var('param') }}` parameter reference conversion
  - Primary key inference from field names (`BK_`, `_KEY`, `_ID`, `_PK` patterns) and port types
  - Richer unit test generation with DML_TYPE-aware fixtures, filter tests, aggregation tests
  - `--tier` CLI filter to process only specific tiers
  - Summary CSV and per-model conversion log output
- **Agent 8: Source-Target Extractor** (`agents/informatica-source-target/`) — new agent for impact analysis
  - Extracts source/target table mappings from PowerCenter XML into flat CSV
  - Reuses `PowerCenterParser` from `infa-xml-parser` agent
  - Output format: `workflow_name, type (SOURCE/TARGET), table_name`
  - Deduplication of source/target pairs across multi-target mappings
- **GAPS.md** — comprehensive gap analysis document from scanning 34,503 production XMLs
  - Transformation type name mismatches and coverage summary
  - Unhandled XML element inventory (Critical/High/Medium/Low severity)
  - Database type coverage analysis (Teradata dominant at 80,042 instances)
  - Task type coverage (Command tasks: 16,447 instances)
  - SQL override prevalence (50,337 Source Qualifier overrides)
  - Prioritized P0–P3 remediation plan (20 items)

### Changed
- `parse_pc_xml.py` rewritten from procedural functions to `PowerCenterParser` class with `parse()` entry point
- Transformation type normalization now happens at extraction time via `_normalize_transform_type()`
- Connector graph resolution uses `instance_map` for instance→transform lookup instead of name-only matching
- Corpus coverage assessment uses canonical (normalized) type names
- `agent2_batch.py` generates `generated_by: 'INFA2DBT_accelerator_v2.0.0'` in model metadata (to be bumped in next release)

### Fixed
- **Critical: Transformation type mismatches** — `Lookup` (1,773 instances) and `Sequence Generator` (492 instances) were silently classified as "missing" corpus coverage because the parser expected different TYPE string values than what appears in actual XML exports. Fixed with `TYPE_ALIASES` map.
- **Instance-to-transform resolution** — Connector graph tracing could fail when instance names differed from transformation names. Fixed by extracting `INSTANCE` elements and building `instance_map` for proper resolution.
- **Malformed XML handling** — 50 files in `EDWPROD-3/EDWTD_P2B_*` failed parsing due to invalid tokens (unescaped ampersands, embedded quotes). Fixed with `_sanitize_xml()` pre-processor that escapes bare ampersands and fixes attribute quote mismatches.

---

## [2.1.0] - 2026-03-19

### Added
- **Collision-Free Model Naming Convention** to prevent dbt model overwrites
  - Format: `{xml_filename}_{seq:02d}_{stg|int|mart}_{target_name}`
  - XML filename prefix guarantees uniqueness across workflows (e.g., `edwtd_gl_wf_ff_mf_ap_invoice_lines_all_01_mart_ff_mf_ap_invoice_lines_all`)
  - Zero-padded sequence number (`01`, `02`, ...) preserves target execution order within each workflow
  - Resolves critical bug where multiple Informatica workflows writing to the same target table caused the last dbt model to overwrite earlier ones (1,322 handoffs → only 1,003 unique files in v2.0.0)
- **Offline / No-State Mode** for Agent 2
  - `--no-state` CLI flag skips Snowflake state persistence
  - `INFA2DBT_NO_STATE=1` environment variable bypasses Snowflake connection entirely in `snowflake_connection()` context manager
  - Workaround for `~/.snowflake/connections.toml` TOML parsing errors (e.g., `EmptyKeyError` at malformed lines)

### Changed
- Agent 1 (`agent1_parser.py`): `proposed_model_name` now uses XML filename as workflow prefix instead of mapping name
  - Previous: `generate_model_name(target_name)` → e.g., `mart_cap_accounts`
  - New: `{xml_basename}_{seq:02d}_{base_model_name}` → e.g., `edwtd_gl_wf_ff_cap_accounts_01_mart_cap_accounts`
  - Sequence counter is global per XML file (not per mapping), ensuring uniqueness across multi-mapping workflows
- Agent 2 (`agent2_converter_v2.py`): `determine_materialization()` updated to detect `stg_`/`int_`/`mart_` prefixes anywhere in the model name (not just at the start)
  - Previous: `model_name.startswith('stg_')` → always returned `'table'` for prefixed names
  - New: checks both `startswith()` and `'_stg_' in model_name` patterns
- Agent 1 SKILL.md: Updated naming convention documentation (Step 8)
- Agent 2 SKILL.md: Updated materialization rules to reflect substring matching
- USAGE.md: Added `--no-state` and `INFA2DBT_NO_STATE` documentation, TOML troubleshooting
- Version bumped to 2.1.0 across all skill files and config

### Fixed
- **Critical: Model overwrite bug** — When multiple Informatica workflows wrote to the same target table, the last dbt model file silently overwrote earlier ones. In a batch of 803 XMLs producing 1,322 targets, only 1,003 unique `.sql` files were generated (319 lost). Fixed by prefixing model names with the unique XML filename.
- **Residual 8-file collision** — Initial fix using `wf_{mapping_name}` as prefix still produced 8 collisions (1,314 vs 1,322) because different XML files can share the same mapping name. Switched to XML filename (`Path(xml_path).stem`) which is guaranteed unique per workflow.
- **Materialization always returning 'table'** — With the new naming convention, `stg_`/`int_` prefixes moved to mid-string, causing `determine_materialization()` to miss them. Updated pattern matching to check for `_stg_`/`_int_` substrings.
- **Snowflake connector TOML parse error** — `snowflake.connector.connect()` called before `--no-state` flag was evaluated, triggering `tomlkit.exceptions.EmptyKeyError` on malformed `connections.toml`. Fixed by adding early `INFA2DBT_NO_STATE` env var check in the `snowflake_connection()` context manager.

---

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
- **Target Consolidation Detection** (hybrid approach)
  - Agent 2: Real-time warning when target table already has models in registry
  - Agent 6: Deep consolidation analysis with SQL similarity scoring (Step 11)
  - Identifies duplicate models from multiple Informatica workflows → same target
  - Recommendations: MERGE (>70% similarity), REVIEW (40-70%), KEEP SEPARATE (<40%)

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
