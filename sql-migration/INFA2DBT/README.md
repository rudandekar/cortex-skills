# INFA2DBT Accelerator

7-agent pipeline for converting Informatica PowerCenter workflows to dbt models on Snowflake.

## Quick Start

```bash
# 1. Set up Snowflake infrastructure (optional - for state persistence)
snowsql -f scripts/setup_infrastructure.sql

# 2. Parse Informatica XML
python3 src/agent1_parser.py --xml-dir ./xml_exports --output-dir ./artifacts/handoffs

# 3. Convert to dbt (with RAG-enhanced corpus lookup)
python3 src/agent2_converter_v2.py --handoff-dir ./artifacts/handoffs --output-dir ./artifacts/output --no-state

# 4. Run dbt models
cd artifacts/output && dbt run
```

## Test Run Results

Tested on 3 sample XML files:

| Stage | Input | Output | Status |
|-------|-------|--------|--------|
| Agent 1: Parser | 3 XMLs | 6 handoffs | ✓ |
| Agent 2: Converter | 6 handoffs | 6 dbt models | ✓ |

Generated models with 84-94% fidelity scores (avg 85.7%).

## Documentation

| File | Description |
|------|-------------|
| [SKILL.md](SKILL.md) | Main orchestrator skill specification |
| [MIGRATION.md](MIGRATION.md) | Snowflake infrastructure setup |
| [USAGE.md](USAGE.md) | End-to-end usage guide |
| [DOCUMENTATION.md](DOCUMENTATION.md) | Technical architecture |
| [ROLES_AND_PRIVILEGES.md](ROLES_AND_PRIVILEGES.md) | Security and RBAC setup |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

## Pipeline Agents

| Agent | Skill | Purpose |
|-------|-------|---------|
| 1 | `infa-xml-parser` | Parse PowerCenter XML |
| 2 | `infa-to-dbt-converter` | Generate dbt SQL (RAG-enhanced) |
| 3 | `conversion-fidelity-scorer` | Score conversion quality |
| 4 | `dbt-validation-critique` | Validate dbt compliance |
| 5 | `phase1-handoff` | Checkpoint management |
| 6 | `roi-subgraph-scorer` | ROI tier analysis |
| 7 | `dbt-sql-optimizer` | Performance optimization |

## Requirements

- Python 3.9+
- snowflake-connector-python (optional, for state persistence)
- dbt-snowflake
- Snowflake account with Cortex Search enabled (optional, for RAG)

## License

Internal use only - Deloitte
