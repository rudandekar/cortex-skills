# SKILL CHANGELOG — sybase-to-snowflake

## [0.3.0] - 2026-04-02
### Added — Snowflake optimization recommendations (Step 12)
- **Step 12: Generate Snowflake optimization recommendations** — rule-based analysis across 8 categories: clustering keys, transient tables, search optimization, query acceleration, informational constraints, dynamic table candidates, streams+tasks roadmap, storage tuning
- **Step 12a: HITL Checkpoint** — 🟡 MEDIUM — approve auto-apply vs manual-review scope before applying optimization DDL
- **NEW reference file: references/optimization-rules.md** — 8 optimization rules (O1–O8) with detection logic, DDL templates, severity classification, applicability conditions, and Snowflake documentation references
- **NEW schema: optimization_report.json** — per-object recommendations with category, severity, DDL, classification (auto-apply vs manual-review)
- **NEW output: optimization_ddl.sql** — ready-to-execute DDL grouped by category, auto-apply first then manual-review commented out
- Updated Output Summary Template with optimization recommendation counts
- Updated Files Produced table (16 → 18 files)
- Updated HITL Checkpoints Summary (4 → 5 checkpoints)
- Updated Reference Files section
- Updated Example Invocation with steps 12–13
- Updated conversion-standards.md with section 9 (Optimization Report Schema) and expanded Validation Checklist
- Updated example-01 with full optimization output: 22 recommendations (14 auto-apply, 8 manual-review) across all 8 categories

### Changed
- SKILL.md expanded from 398 lines (v0.2.0) to 467 lines (v0.3.0)
- Workflow expanded from 11 steps to 12 steps (+ HITL checkpoint 12a)
- conversion-standards.md expanded from 537 lines to 624 lines
- run_log.json and conversion_report.json updated with optimization step and summary

### Fixed — v0.3.0 consistency pass
- Bumped all stale `v0.2.0` version references to `v0.3.0` in SKILL.md meta header template, conversion-standards.md meta header example, 7 JSON schema examples in conversion-standards.md, complexity-scoring-rubric.md header, and evals.json top-level version
- Added `v0.3.0` version reference to sybase-to-snowflake-type-map.md header (was missing) and optimization-rules.md header
- Updated step count references from 11 to 12: conversion-standards.md `step_id` range, Canonical Step Names table (added Step 12 + 12a), evals.json eval #5 assertion
- Added eval #8 for Step 12 optimization recommendations with 9 assertions
- Updated eval #5 assertions to include optimization_report.json and optimization_ddl.sql as required outputs
- Fixed `conversion_report.md` → `conversion_report.json` throughout: SKILL.md (Step 11 description, output table, error handling, example invocation), evals.json eval #5 assertion, PIPELINE_SUMMARY.md artifacts table
- Updated PIPELINE_SUMMARY.md: added Step 12/12a to pipeline table, updated step count from 11 to 12, added optimization artifacts to key artifacts table, added optimization-rules.md to reference materials, updated execution environment summary
- Added illustrative-counts note to optimization report schema example in conversion-standards.md

## [0.2.0] - 2026-04-01
### Added — INFA2DBT leading practices alignment
- **Guiding Principles section** — 6 principles: semantic equivalence, self-documenting output, structured handoffs, retry before quarantine, never drop silently, provenance in every file
- **Security Hardening section** — compile-only validation, no production data in tests, dollar-quote escaping
- **Invocation Modes** — Full Pipeline, Step-Specific, Resume from Failure (with run_log.json)
- **Structured handoff JSON contracts** — 7 artifacts: object_inventory.json, dependency_graph.json, complexity_report.json, conversion_log.json, fidelity_scores.json, quarantine_inventory.json, run_log.json
- **Conversion annotations** — inline `-- [CONVERT]` and `-- [REMOVE]` comments with type-map section references
- **Standardized TODO blocks** — template for unsupported constructs with construct name, source line, and suggested approach
- **Meta provenance headers** — source, object, complexity, timestamp, applied rules in every generated file
- **Step 7: Fidelity scoring** — 5 weighted dimensions (row_count 30%, column_match 15%, null_profile 15%, aggregate_match 25%, spot_check 15%) with PASS/RETRY/QUARANTINE routing
- **Per-step audit logging** — structured JSON log entries aggregated into run_log.json
- **Output summary template** — ASCII-bordered status report
- **NEW reference file: references/conversion-standards.md** — handoff JSON schemas, TODO block template, audit log format, meta header template, fidelity scoring contract, quarantine entry format, run log schema
- **Updated references/complexity-scoring-rubric.md** — fidelity threshold adjustment by tier, quarantine routing rules, retry budget, quarantine severity classification, pre-quarantine flagging
- **Updated examples/example-01-insurance-dw/README.md** — expected fidelity scoring outcomes per object, retry/quarantine patterns, edge case seed data requirements
- **Updated evals/evals.json** — expanded from 4 to 7 test cases; new assertions for handoffs, fidelity scoring, annotations, meta provenance, and resume-from-failure
- Output specification expanded from 9 to 16 files

### Changed
- SKILL.md rewritten from 297 lines (v0.1.0) to 399 lines (v0.2.0)
- Workflow expanded from 10 steps to 11 steps (new Step 7: fidelity scoring)
- Error handling section enhanced with fidelity-based routing and quarantine

## [0.1.0] - 2026-04-01
### Initial release (draft)
- Created skill directory structure
- SKILL.md with 10-step workflow (7 logical agents + 3 additional HITL/validation steps)
- references/sybase-to-snowflake-type-map.md covering data types, functions, patterns, system objects, control flow, and format codes
- references/complexity-scoring-rubric.md with 7-dimension weighted scoring
- Example 01: P&C insurance data warehouse ETL (5 Sybase SQL scripts)
- evals/evals.json with 4 test prompts and assertions
- Status: draft — requires execution against real input and output review before promotion to active
