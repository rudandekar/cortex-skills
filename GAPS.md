# INFA2DBT Parser Coverage Gap Analysis

**Scan Date:** 2026-03-18
**Codebase:** /mnt/c/Users/rdandeka/XMLs (6 subdirectories)
**Total XML Files:** 34,503
**Valid (parseable):** 34,453 (99.86%)
**Malformed XML:** 50 (all in EDWPROD-3/EDWTD_P2B_* — invalid tokens)

---

## 1. Transformation Type Gaps

The parser's `STANDARD_TRANSFORMS` set uses names that don't match the actual XML `TYPE` attribute values in this codebase. This is the **highest-impact gap**.

### 1a. Name Mismatches (parser has the concept but wrong TYPE string)

| Parser Expects | Actual XML TYPE Value | Instances | Impact |
|---|---|---|---|
| `Lookup` | `Lookup Procedure` | 1,773 | **CRITICAL** — all lookups silently classified as "missing" |
| `Sequence Generator` | `Sequence` | 492 | **HIGH** — all sequence generators misclassified |

### 1b. Completely Missing Transformation Types

| XML TYPE Value | Instances | Files Affected | Severity | Recommended dbt Pattern |
|---|---|---|---|---|
| `XML Source Qualifier` | 40 | 40 | MEDIUM | `source()` with PARSE_XML / LATERAL FLATTEN |
| `Input Transformation` | 193 | ~193 | MEDIUM | Passthrough (mapplet input port) |
| `Output Transformation` | 193 | ~193 | MEDIUM | Passthrough (mapplet output port) |
| `Mapplet` (as transform) | 193 | ~193 | MEDIUM | Expand inline — resolve to constituent transforms |

### 1c. Coverage Summary

| Status | Types | Total Instances | % of All Transforms |
|---|---|---|---|
| Correctly Handled | 8 of 14 standard | 92,244 | 96.6% |
| Name Mismatch (broken) | 2 | 2,265 | 2.4% |
| Missing from parser | 4 | 619 | 0.6% |
| Escalate (correct) | 2 of 3 found | 319 | 0.3% |

---

## 2. Unhandled XML Element Gaps

Elements present in the codebase that the parser does **not** extract or process.

### 2a. Critical — Affect Parsing Correctness

| Element | Count | Files | Gap Description |
|---|---|---|---|
| `WORKLET` | 3,420 | 1,866 | Sub-workflows containing sessions/tasks. Parser ignores worklet-scoped sessions, causing **missing targets** in worklet-heavy workflows. |
| `SHORTCUT` | 3,774 | 2,036 | References to shared mappings in other folders (all 3,774 are OBJECTTYPE=MAPPING). Parser cannot resolve cross-folder mapping references. |
| `ASSOCIATED_SOURCE_INSTANCE` | 71,571 | 34,290 | Links Source Qualifier instances to their source definitions. Parser relies on connector tracing instead, which can miss multi-source SQ joins. |
| `INSTANCE` | 234,704 | all | Mapping instances carry `TRANSFORMATION_TYPE` and `TRANSFORMATION_NAME`. Parser extracts transforms but ignores instances, losing the instance→transform resolution needed for connector graph accuracy. |
| `TARGETLOADORDER` | 67,635 | 34,326 | Specifies load ordering for multi-target mappings. Parser's topological sort is workflow-level only; within a mapping, target load order is lost. |

### 2b. High — Affect dbt Configuration Quality

| Element | Count | Files | Gap Description |
|---|---|---|---|
| `MAPPINGVARIABLE` | 143,655 | 28,817 | Mapping parameters ($$param). Not extracted — dbt `var()` / env_var() mappings are lost. |
| `WORKFLOWVARIABLE` | 953,257 | 34,246 | Workflow variables often carry connection strings, file paths, and runtime parameters. Not extracted for dbt config. |
| `FLATFILE` | 56,739 | 15,837 | Flat file source/target definitions. Parser only extracts `SOURCE`/`TARGET` database objects. **15,837 files** (45.9%) use flat files. |
| `CONFIG` / `CONFIGREFERENCE` | 34,737 / 55,431 | 34,392 | Session configuration (commit interval, error handling, buffer size). Lost — affects incremental strategy and error handling in dbt. |
| `PARTITION` | 46,092 | 11,582 | Session partitioning (up to 40 partitions found). Relevant for dbt cluster_by / partition hints on Snowflake. |
| `SESSIONCOMPONENT` | 17,312 | 5,394 | Reader/Writer overrides (connection, SQL override at session level). Can override mapping-level SQL. |
| `GROUP` | 2,554 | 538 | Router/transformation output groups. Parser handles Router type but doesn't extract GROUP definitions for output group names. |

### 2c. Medium — Metadata / Audit

| Element | Count | Files | Gap Description |
|---|---|---|---|
| `ERPINFO` | 49,876 | 34,336 | ERP application metadata. Useful for source system tagging in dbt. |
| `SCHEDULER` / `SCHEDULEINFO` | 34,245 | 34,245 | Schedule definitions (daily, weekly, recurring). Could improve freq_hint inference beyond name pattern matching. |
| `SESSTRANSFORMATIONINST` | 252,352 | 34,327 | Session-level overrides per transformation instance. Can override SQL, connections, properties at runtime. |
| `METADATAEXTENSION` | 7,587 | 4,012 | Custom metadata annotations. |
| `FIELDDEPENDENCY` | 9,413 | 96 | Explicit field-to-field dependencies within transformations. |
| `FIELDATTRIBUTE` | 4,340 | n/a | Additional field-level attributes (beyond TRANSFORMFIELD). |
| `VALUEPAIR` | 3,865 | n/a | Name-value configuration pairs. |

### 2d. Low — Rare / Niche

| Element | Count | Files | Description |
|---|---|---|---|
| `XMLTEXT` | 120 | 40 | XML payload data in XML Source Qualifier transforms |
| `XMLINFO` | 40 | 40 | XML source/target metadata |
| `WORKFLOWEVENT` | 8 | 8 | Event-based workflow triggers |
| `EXPRMACRO` | 3 | 3 | Expression macros (reusable expression snippets) |
| `KEYRANGE` | 67 | n/a | Key-range partition definitions |
| `TIMER` | 1 | 1 | Timer task |
| `TARGETINDEX` | 2 | 2 | Target index definitions |
| `TRANSFORMFIELDATTR` | 80 | n/a | Extended field attributes on transforms |
| `CONWFRUNINFO` | 519 | n/a | Concurrent workflow run configuration |
| `INITPROP` | 438 | n/a | Initialization properties |

---

## 3. Database Type Coverage

| Database Type | Instances (Source+Target) | Notes |
|---|---|---|
| Teradata | 80,042 | **Dominant** — parser has no Teradata-specific SQL dialect handling |
| Flat File | 20,426 | Parser ignores flat file definitions entirely |
| Oracle | 15,982 | Parser assumes Oracle only |
| XML | 40 | XML Source Qualifier needed |
| Microsoft SQL Server | 36 | Minor |
| ODBC | 34 | Generic |

---

## 4. Task Type Coverage

Workflow TASK types found but not processed by parser:

| Task TYPE | Count | Impact |
|---|---|---|
| `Start` | 37,665 | Low — implicit |
| `Command` | 16,447 | **HIGH** — pre/post SQL commands, shell scripts. Maps to dbt pre_hook/post_hook. |
| `Email` | 991 | LOW — notification, no dbt equivalent |
| `Decision` | 242 | MEDIUM — conditional branching in workflow. Maps to dbt conditional logic. |
| `Control` | 146 | MEDIUM — stop/abort workflow logic. Maps to dbt on-run-end hooks. |
| `Event Wait` | 5 | LOW |
| `Timer` | 1 | LOW |
| `Assignment` | 1 | LOW |

---

## 5. SQL Override Prevalence

| Feature | Count | Notes |
|---|---|---|
| Source Qualifier SQL Override | 50,337 | **Massive** — parser extracts these but as table attributes only. Need dedicated SQL parsing for Teradata dialect. |
| Lookup SQL Override | 572 | Moderate — lookup overrides need their own CTE generation. |
| Expression Macros | 3 | Minimal |

---

## 6. Malformed XML Files

50 files in `EDWPROD-3/EDWTD_P2B_*` fail XML parsing with "not well-formed (invalid token)" errors. These need pre-processing (likely ampersand/special character escaping) before they can be parsed.

---

## 7. Prioritized Remediation Plan

### P0 — Fix Immediately (breaks existing functionality)
1. **Fix transformation type name mismatches**: `Lookup` → `Lookup Procedure`, `Sequence Generator` → `Sequence`
2. **Add `INSTANCE` element extraction** for proper instance→transform resolution in connector graph

### P1 — High Impact Additions
3. **Add `WORKLET` extraction** (1,866 files affected) — expand worklet sessions into main workflow
4. **Add `SHORTCUT` resolution** (2,036 files) — resolve cross-folder mapping references
5. **Add `FLATFILE` extraction** (15,837 files / 45.9%) — flat file source/target definitions
6. **Add `ASSOCIATED_SOURCE_INSTANCE`** — correct multi-source SQ resolution
7. **Add `MAPPINGVARIABLE` / `WORKFLOWVARIABLE` extraction** — map to dbt var()
8. **Add `Command` task type extraction** — map to pre_hook/post_hook
9. **Add `TARGETLOADORDER` extraction** — intra-mapping target ordering
10. **Add missing transforms**: `XML Source Qualifier`, `Input Transformation`, `Output Transformation`, `Mapplet` (as transform type)

### P2 — Quality Improvements
11. **Add `CONFIG` / `CONFIGREFERENCE` extraction** for session config
12. **Add `PARTITION` extraction** for parallelism hints
13. **Add `SESSIONCOMPONENT` extraction** for reader/writer overrides
14. **Add `SCHEDULER` / `SCHEDULEINFO` extraction** to improve freq_hint
15. **Add `GROUP` extraction** for Router output groups
16. **Add `SESSTRANSFORMATIONINST` extraction** for session-level overrides

### P3 — Metadata / Nice-to-Have
17. **Add `ERPINFO` extraction** for source system tagging
18. **Add `METADATAEXTENSION` extraction**
19. **Add `FIELDDEPENDENCY` extraction**
20. **Add malformed XML pre-processor** for the 50 broken files
