# Cortex Code Custom Skills

Custom skills for Snowflake Cortex Code CLI.

## Installation

```bash
# Clone to your Cortex Code skills directory
git clone <repo-url> ~/.snowflake/cortex/skills

# Or clone to a specific location and symlink
git clone <repo-url> ~/cortex-skills
ln -s ~/cortex-skills/* ~/.snowflake/cortex/skills/
```

## Available Skills

### Healthcare Analytics Suite

| Skill | Description | Triggers |
|-------|-------------|----------|
| [hipaa-phi-governance](./hipaa-phi-governance/) | Two-zone PHI architecture with masking, RBAC, offshore access | HIPAA, PHI, masking, offshore, India access |
| [cortex-ml-classification](./cortex-ml-classification/) | Build classification models with Cortex ML | predict, no-show, churn, fraud, ML model |
| [operational-action-queue](./operational-action-queue/) | Human-in-the-loop Streamlit dashboards | action queue, approve, override, HITL |
| [healthcare-analytics-accelerator](./healthcare-analytics-accelerator/) | End-to-end project orchestration | healthcare project, EPIC, crawl-walk-run |

### SQL Migration

| Skill | Description | Triggers |
|-------|-------------|----------|
| [SnowConvertLegacySQL](./SnowConvertLegacySQL/) | Convert legacy SQL to Snowflake-native | SQL Server, Oracle, Teradata migration |
| [ConvertLegacySQL](./ConvertLegacySQL/) | Transform converted SQL to dbt models | dbt transformation |

## Usage

Skills auto-trigger based on keywords, or invoke explicitly:

```
/skill hipaa-phi-governance
/skill cortex-ml-classification
/skill operational-action-queue
/skill healthcare-analytics-accelerator
```

## Directory Structure

```
~/.snowflake/cortex/skills/
├── README.md
├── hipaa-phi-governance/
│   └── SKILL.md
├── cortex-ml-classification/
│   └── SKILL.md
├── operational-action-queue/
│   └── SKILL.md
├── healthcare-analytics-accelerator/
│   └── SKILL.md
├── SnowConvertLegacySQL/
│   └── SKILL.md
└── ConvertLegacySQL/
    └── SKILL.md
```

## Contributing

1. Create a new skill directory: `mkdir my-new-skill`
2. Add `SKILL.md` following the [Cortex Code skill format](https://docs.snowflake.com/en/developer-guide/cortex-code/skills)
3. Keep skills under 500 lines
4. Include frontmatter with `name` and `description` (triggers)
5. Add stopping points (`⚠️ STOP`) for user checkpoints

## Skill Design Documentation

Detailed design specifications for the healthcare skills are in:
- `/home/rudandekar/agile-capacity-docs/skills/`

These include testing frameworks, optimization guidance, and validation checklists.
