---
name: healthcare-analytics-accelerator
description: "Orchestrate end-to-end healthcare analytics projects using crawl-walk-run methodology. Use when: starting a healthcare analytics project, setting up EPIC/Cerner/EHR analytics, planning a multi-week healthcare AI sprint, coordinating governance + ML + dashboard workstreams. Triggers: healthcare project, EPIC, Cerner, EHR analytics, crawl walk run, sprint plan, healthcare AI, clinical analytics, patient analytics, capacity planning, population health, readmission, no-show project."
---

# Healthcare Analytics Accelerator

## Overview

This skill orchestrates complete healthcare analytics projects by invoking specialized sub-skills:
- `healthcare/hipaa-phi-governance` → Foundation phase (HIPAA compliance, PHI masking)
- `generic/cortex-ml-classification` → Crawl phase (predictive models)
- `generic/operational-action-queue` → Walk phase (human-in-the-loop dashboard)

## Workflow

### Step 1: Project Discovery

**Ask** user: Project name, primary use case (No-Show / Readmission / Revenue Cycle / Population Health / Custom), source EHR system (EPIC Clarity / Cerner Millennium / Meditech / AllScripts / Custom EDW), and data ingestion method (IDMC / OpenFlow / Other).

**⚠️ STOP**: Wait for user response.

### Step 2: Team & Access

**Ask** user: Offshore team involvement (Yes-India / Yes-Other / No-US only), key team members (DE Lead, Analytics Lead, Business Sponsor), sprint duration, and start date.

**⚠️ STOP**: Wait for user response.

### Step 3: Infrastructure

**Ask** user: Target Snowflake database name, warehouse size (XS/S/M/L), Streamlit availability, Cortex ML availability.

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Project Plan

Based on inputs, generate a customized plan with four phases:

**Foundation (Phase 1):** Environment setup (database, schemas, warehouse), Governance (invoke `hipaa-phi-governance`), Data integration setup.

**Crawl (Phase 2):** Data landing + CDC validation, Dimensional model (dims + facts + views), ML model (invoke `cortex-ml-classification`), Internal demo.

**Walk (Phase 3):** Dashboard (invoke `operational-action-queue`), Pilot deployment (user access, training, support channel), Iteration on feedback.

**Run (Phase 4):** Outcomes assessment (actioned vs control comparison), Production readiness (docs, runbook, on-call), Handoff and knowledge transfer.

**⚠️ STOP**: Get approval before proceeding.

### Step 5: Foundation Phase

#### 5.1 Environment Setup
```sql
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS <database_name>;
CREATE SCHEMA IF NOT EXISTS <database_name>.RAW;
CREATE SCHEMA IF NOT EXISTS <database_name>.STAGING;
CREATE SCHEMA IF NOT EXISTS <database_name>.ANALYTICS;
CREATE WAREHOUSE IF NOT EXISTS <project>_WH
    WAREHOUSE_SIZE = '<size>' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;
```

#### 5.2 Governance
If offshore access required, **Load** `hipaa-phi-governance` skill and follow its workflow.

**⚠️ STOP**: Complete governance setup before proceeding.

#### 5.3 Foundation Gate Review
Checklist: database/schemas created, warehouse configured, RBAC roles created, masking policies applied (if offshore), network policies configured (if offshore), classification profile enabled, data connection tested.

### Step 6: Crawl Phase

Confirm data has landed via IDMC/OpenFlow. Validate with:
```sql
SELECT table_name, row_count FROM information_schema.tables WHERE table_schema = 'RAW';
```

**Load** `cortex-ml-classification` skill for model development.

**⚠️ STOP**: Complete model development. Gate review: data loaded, dimensional model created, ML model trained with acceptable metrics, scoring pipeline deployed.

### Step 7: Walk Phase

**Load** `operational-action-queue` skill for dashboard development.

Grant pilot user access:
```sql
GRANT ROLE <analyst_role> TO USER <pilot_user>;
```

**⚠️ STOP**: Confirm pilot is live. Gate review: dashboard deployed, users have access, training completed, feedback being collected.

### Step 8: Run Phase

Outcomes assessment:
```sql
SELECT 
    CASE WHEN a.entity_id IS NOT NULL THEN 'ACTIONED' ELSE 'CONTROL' END AS cohort,
    COUNT(*) AS total,
    SUM(<positive_outcome>) AS positive_outcomes,
    ROUND(positive_outcomes * 100.0 / total, 2) AS rate
FROM <outcomes_table> o
LEFT JOIN <schema>.ACTION_LOG a ON o.<entity_id> = a.entity_id
GROUP BY 1;
```

Final gate review: outcomes comparison completed, impact metrics documented, executive summary prepared, all P1 issues resolved, documentation complete, production decision made.

## Stopping Points

- ✋ Step 1-3: After each discovery step
- ✋ Step 4: After project plan review
- ✋ Step 5-8: After each phase gate review

## Output

- Complete project plan with phase structure
- Database and schema structure
- Governance controls (via `hipaa-phi-governance`)
- ML model and scoring pipeline (via `cortex-ml-classification`)
- Operational dashboard (via `operational-action-queue`)
- Phase gate checklists and outcomes assessment

## Use Case Templates

| Use Case | Target Variable | Key Features | Success Metric |
|----------|-----------------|--------------|----------------|
| No-Show | NOSHOW_FLAG | Historical rate, lead time, day of week | Slot utilization |
| Readmission | READMITTED_30D | Diagnosis, LOS, prior admits | Readmission rate |
| Denials | DENIED_FLAG | Payer, CPT, prior denial rate | Denial rate |

## Related Skills

- `hipaa-phi-governance` - Foundation phase
- `cortex-ml-classification` - Crawl phase
- `operational-action-queue` - Walk phase
