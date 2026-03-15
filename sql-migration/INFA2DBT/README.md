# INFA2DBT Accelerator

7-agent pipeline for converting Informatica PowerCenter workflows to dbt models on Snowflake.

## Quick Start

```bash
# 1. Set up Snowflake infrastructure
snowsql -f scripts/setup_infrastructure.sql

# 2. Parse Informatica XML
python src/agent1_parser.py --xml-dir ./xml_exports --output-dir ./artifacts/handoffs

# 3. Convert to dbt (with RAG-enhanced corpus lookup)
python src/agent2_converter_v2.py ./artifacts/handoffs/workflow_handoff.json

# 4. Run dbt models
cd dbt_project && dbt run
```

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
- snowflake-connector-python
- dbt-snowflake
- Snowflake account with Cortex Search enabled

## License

Internal use only - Deloitte
