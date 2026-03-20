---
name: informatica-source-target
version: 1.0.0
tier: user
author: Deloitte FDE Practice
created: 2026-03-19
last_updated: 2026-03-19
status: active

description: >
  Extracts source and target table mappings from Informatica PowerCenter XML
  exports and produces a flat CSV suitable for impact analysis, lineage audits,
  and migration planning.

  TRIGGER CONDITIONS (require "Informatica" OR "PowerCenter" alongside):
  - Keywords: "source target extract", "source target CSV", "lineage CSV",
    "table mapping", "impact analysis", "INFA source target"
  - User asks for a flat list of which sources feed which targets

  DO NOT trigger on generic "parse XML" requests without Informatica context.

  OUTPUT: Single CSV with columns workflow_name, type (SOURCE/TARGET), table_name.

compatibility:
  tools: [bash, read, write]
  context: [CLAUDE.md, PROJECT.md]
  scripts:
    - scripts/extract_source_target.py
---

# Informatica Source-Target Extractor

Extracts source and target table names from Informatica PowerCenter XML exports
into a single flat CSV. Reuses `PowerCenterParser` from the `infa-xml-parser`
agent.

## Inputs

- **Required:** `--xml-dir <path>` (directory of XMLs) OR `--xml-file <path>` (single XML)
- **Required:** `--output <path>` — output CSV path

## Output Format (CSV)

```
workflow_name,type,table_name
wf_TD_to_TD,SOURCE,SRC_DB.SRC_TD_TABLE
wf_TD_to_TD,TARGET,TGT_TD_TABLE
```

## Usage

```bash
python scripts/extract_source_target.py --xml-file /path/to/workflow.xml --output source_target.csv
python scripts/extract_source_target.py --xml-dir /path/to/xmls/ --output source_target.csv
```
