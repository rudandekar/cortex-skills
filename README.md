# Cortex Code Custom Skills

Custom skills for Snowflake Cortex Code CLI, organized by domain.

## Installation

```bash
# Clone to your Cortex Code skills directory
git clone https://github.com/rudandekar/cortex-skills.git ~/.snowflake/cortex/skills

# Or clone to a specific location and symlink
git clone https://github.com/rudandekar/cortex-skills.git ~/cortex-skills
ln -s ~/cortex-skills/* ~/.snowflake/cortex/skills/
```

## Directory Structure

```
~/.snowflake/cortex/skills/
├── README.md
│
├── generic/                              # Universal skills (any industry)
│   ├── sensitive-data-governance/        # Two-zone architecture, masking, RBAC
│   ├── cortex-ml-classification/         # Predictive ML with Cortex ML
│   └── operational-action-queue/         # Human-in-the-loop Streamlit dashboards
│
├── healthcare/                           # Healthcare-specific skills
│   ├── hipaa-phi-governance/             # HIPAA/PHI compliance (extends generic)
│   └── healthcare-analytics-accelerator/ # End-to-end project orchestration
│
└── sql-migration/                        # SQL conversion skills
    ├── SnowConvertLegacySQL/             # Legacy SQL to Snowflake
    └── ConvertLegacySQL/                 # SQL to dbt models
```

## Available Skills

### Generic Skills (Universal)

| Skill | Description | Triggers |
|-------|-------------|----------|
| [sensitive-data-governance](./generic/sensitive-data-governance/) | Two-zone architecture for any sensitive data (PII, PCI, GDPR) | data governance, masking, PII, offshore access |
| [cortex-ml-classification](./generic/cortex-ml-classification/) | Build classification models with Cortex ML | predict, churn, fraud, ML model, classification |
| [operational-action-queue](./generic/operational-action-queue/) | Human-in-the-loop Streamlit dashboards | action queue, approve, override, HITL |

### Healthcare Skills

| Skill | Description | Triggers |
|-------|-------------|----------|
| [hipaa-phi-governance](./healthcare/hipaa-phi-governance/) | HIPAA-compliant PHI protection with Safe Harbor masking | HIPAA, PHI, healthcare governance, Safe Harbor |
| [healthcare-analytics-accelerator](./healthcare/healthcare-analytics-accelerator/) | End-to-end healthcare project orchestration | healthcare project, EPIC, Cerner, crawl-walk-run |

### SQL Migration Skills

| Skill | Description | Triggers |
|-------|-------------|----------|
| [SnowConvertLegacySQL](./sql-migration/SnowConvertLegacySQL/) | Convert legacy SQL to Snowflake-native | SQL Server, Oracle, Teradata migration |
| [ConvertLegacySQL](./sql-migration/ConvertLegacySQL/) | Transform converted SQL to dbt models | dbt transformation |

## Usage

Skills auto-trigger based on keywords, or invoke explicitly:

```bash
# Generic skills
/skill sensitive-data-governance
/skill cortex-ml-classification
/skill operational-action-queue

# Healthcare skills
/skill hipaa-phi-governance
/skill healthcare-analytics-accelerator

# SQL migration
/skill SnowConvertLegacySQL
/skill ConvertLegacySQL
```

## Skill Relationships

```
┌─────────────────────────────────────────────────────────────────┐
│                  healthcare-analytics-accelerator               │
│                    (orchestrates full project)                  │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ hipaa-phi-      │  │ cortex-ml-      │  │ operational-    │
│ governance      │  │ classification  │  │ action-queue    │
│ (Foundation)    │  │ (Crawl)         │  │ (Walk)          │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │
         ▼
┌─────────────────┐
│ sensitive-data- │
│ governance      │
│ (generic base)  │
└─────────────────┘
```

## Contributing

1. Choose the appropriate domain folder (`generic/`, `healthcare/`, `sql-migration/`, or create new)
2. Create skill directory: `mkdir <domain>/<skill-name>`
3. Add `SKILL.md` following the [Cortex Code skill format](https://docs.snowflake.com/en/developer-guide/cortex-code/skills)
4. Keep skills under 500 lines
5. Include frontmatter with `name` and `description` (triggers)
6. Add stopping points (`⚠️ STOP`) for user checkpoints
7. Reference related skills where appropriate

## Skill Design Documentation

Detailed design specifications (testing, optimization, validation) are maintained separately in:
- Project documentation repositories
- `/home/rudandekar/agile-capacity-docs/skills/` (local reference)

## License

Internal use - Deloitte
