---
name: healthcare-analytics-accelerator
description: "Orchestrate end-to-end healthcare analytics projects using crawl-walk-run methodology. Use when: starting a healthcare analytics project, setting up EPIC/Cerner/EHR analytics, planning a multi-week healthcare AI sprint, coordinating governance + ML + dashboard workstreams. Triggers: healthcare project, EPIC, Cerner, EHR analytics, crawl walk run, sprint plan, healthcare AI, clinical analytics, patient analytics, capacity planning, population health, readmission, no-show project."
---

# Healthcare Analytics Accelerator

## Overview

This skill orchestrates complete healthcare analytics projects by invoking specialized sub-skills:
- `hipaa-phi-governance` → Foundation phase
- `cortex-ml-classification` → Crawl phase
- `operational-action-queue` → Walk phase

## Workflow

### Step 1: Project Discovery

**Ask** user:
```
Let's set up your healthcare analytics project:

1. Project name? (e.g., "agile-capacity", "readmission-risk")

2. Primary use case?
   a) No-Show / Capacity Management
   b) Readmission Risk
   c) Revenue Cycle / Denials
   d) Population Health
   e) Custom: ___

3. Source EHR system?
   a) EPIC Clarity
   b) Cerner Millennium
   c) Meditech
   d) AllScripts
   e) Custom EDW

4. How is data ingested into Snowflake?
   a) IDMC → Snowflake
   b) IDMC → S3 → External Tables
   c) OpenFlow
   d) Other
```

**⚠️ STOP**: Wait for user response.

### Step 2: Team & Access

**Ask** user:
```
Team configuration:

1. Are offshore teams involved in development?
   a) Yes - India
   b) Yes - Other offshore
   c) No - US only

2. Key team members:
   - Data Engineering Lead: ___
   - Analytics/ML Lead: ___
   - Business/Clinical Sponsor: ___

3. Sprint duration?
   a) 4 weeks
   b) 6 weeks
   c) 8 weeks
   d) 12 weeks

4. Sprint start date?
```

**⚠️ STOP**: Wait for user response.

### Step 3: Infrastructure

**Ask** user:
```
Infrastructure setup:

1. Target Snowflake database name?
   (e.g., AGILE_CAPACITY_DEMO)

2. Target warehouse size?
   a) X-Small (dev/test)
   b) Small (small data)
   c) Medium (recommended)
   d) Large (big data)

3. Is Streamlit in Snowflake available?
   a) Yes
   b) No (need to enable)

4. Is Cortex ML available?
   a) Yes
   b) No (need to enable)
```

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Project Plan

Based on inputs, **generate** customized timeline:

```
📅 <Project Name> - <Sprint Duration> Sprint Plan

FOUNDATION (Weeks 1-2)
├── Environment Setup
│   ├── Create database: <database_name>
│   ├── Create schemas: RAW, STAGING, ANALYTICS
│   └── Configure warehouse
├── Governance [Invoke: hipaa-phi-governance]
│   ├── RBAC roles and grants
│   ├── Classification profile
│   ├── Masking policies
│   └── Network policies (if offshore)
└── Data Integration
    ├── Configure IDMC/OpenFlow connection
    ├── Initial table mapping
    └── Test connectivity

CRAWL (Weeks 3-4)
├── Data Landing
│   ├── Initial load via IDMC/OpenFlow
│   ├── Enable CDC
│   └── Validate row counts
├── Dimensional Model
│   ├── Create dimension tables
│   ├── Create fact tables
│   └── Create analytical views
├── ML Model [Invoke: cortex-ml-classification]
│   ├── Prepare training data
│   ├── Train and evaluate model
│   └── Deploy scoring pipeline
└── Internal Demo
    └── Present to stakeholders

WALK (Weeks 5-6)
├── Dashboard [Invoke: operational-action-queue]
│   ├── Build action queue UI
│   ├── Implement approve/override/defer
│   └── Add analytics tab
├── Pilot Deployment
│   ├── Grant access to pilot users
│   ├── Conduct training session
│   └── Establish support channel
└── Iteration
    ├── Collect feedback
    ├── Fix priority issues
    └── Tune model threshold

RUN (Weeks 7-8)
├── Outcomes Assessment
│   ├── Compare pilot vs control
│   ├── Calculate impact metrics
│   └── Prepare executive summary
├── Production Readiness
│   ├── Documentation complete
│   ├── Runbook finalized
│   └── On-call rotation defined
└── Handoff
    ├── Knowledge transfer
    └── Production decision
```

**Present** plan to user.

**⚠️ STOP**: Get approval before proceeding.

### Step 5: Foundation Phase

**Execute** Foundation checklist:

#### 5.1 Environment Setup
```sql
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS <database_name>
    COMMENT = '<project_name> - Healthcare Analytics';

CREATE SCHEMA IF NOT EXISTS <database_name>.RAW;
CREATE SCHEMA IF NOT EXISTS <database_name>.STAGING;
CREATE SCHEMA IF NOT EXISTS <database_name>.ANALYTICS;

CREATE WAREHOUSE IF NOT EXISTS <project>_WH
    WAREHOUSE_SIZE = '<size>'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

#### 5.2 Governance
**If offshore access required:**
```
Invoking skill: hipaa-phi-governance

This will set up:
- Two-zone architecture (Zone 0 = PHI, Zone 1 = Masked)
- RBAC roles
- Masking policies
- Network policies for India access
```

**Load** `hipaa-phi-governance` skill and follow its workflow.

**⚠️ STOP**: Complete governance setup before proceeding.

#### 5.3 Foundation Gate Review

**Present** checklist:
```
Foundation Phase Checklist:
- [ ] Database and schemas created
- [ ] Warehouse configured
- [ ] RBAC roles created
- [ ] Masking policies applied (if offshore)
- [ ] Network policies configured (if offshore)
- [ ] Classification profile enabled
- [ ] IDMC/OpenFlow connection tested

Ready for Crawl phase? [Yes / No - fix items first]
```

**⚠️ STOP**: Get explicit approval to proceed to Crawl.

### Step 6: Crawl Phase

#### 6.1 Data Landing
Confirm with user that IDMC/OpenFlow has loaded data.

```sql
-- Validate data landed
SELECT table_name, row_count 
FROM information_schema.tables 
WHERE table_schema = 'RAW';
```

#### 6.2 ML Model Development
```
Invoking skill: cortex-ml-classification

This will:
- Prepare training data with feature engineering
- Train classification model
- Evaluate model performance
- Deploy scoring pipeline
```

**Load** `cortex-ml-classification` skill and follow its workflow.

**⚠️ STOP**: Complete model development before proceeding.

#### 6.3 Crawl Gate Review

**Present** checklist:
```
Crawl Phase Checklist:
- [ ] Data loaded and validated
- [ ] Dimensional model created
- [ ] ML model trained
- [ ] Model metrics acceptable (Accuracy: __%, Precision: __%)
- [ ] Scoring pipeline deployed
- [ ] Internal demo completed

Ready for Walk phase? [Yes / No]
```

**⚠️ STOP**: Get approval before Walk phase.

### Step 7: Walk Phase

#### 7.1 Dashboard Development
```
Invoking skill: operational-action-queue

This will:
- Create action queue with approve/override/defer
- Build audit log tables
- Add analytics views
- Deploy Streamlit app
```

**Load** `operational-action-queue` skill and follow its workflow.

#### 7.2 Pilot Deployment

**Ask** user:
```
Pilot configuration:
1. Pilot user list (usernames)?
2. Training session date?
3. Support channel (Teams/Slack)?
```

Grant access to pilot users:
```sql
GRANT ROLE <analyst_role> TO USER <pilot_user_1>;
GRANT ROLE <analyst_role> TO USER <pilot_user_2>;
```

**⚠️ STOP**: Confirm pilot is live before proceeding.

#### 7.3 Walk Gate Review

**Present** checklist:
```
Walk Phase Checklist:
- [ ] Dashboard deployed
- [ ] Pilot users have access
- [ ] Training completed
- [ ] Support channel active
- [ ] Feedback being collected
- [ ] Priority issues addressed

Ready for Run phase? [Yes / No]
```

### Step 8: Run Phase

#### 8.1 Outcomes Assessment

**Generate** comparison queries:
```sql
-- Compare actioned vs non-actioned outcomes
SELECT 
    CASE WHEN a.entity_id IS NOT NULL THEN 'ACTIONED' ELSE 'CONTROL' END AS cohort,
    COUNT(*) AS total,
    SUM(<positive_outcome>) AS positive_outcomes,
    ROUND(positive_outcomes * 100.0 / total, 2) AS rate
FROM <outcomes_table> o
LEFT JOIN <schema>.ACTION_LOG a ON o.<entity_id> = a.entity_id
GROUP BY 1;
```

**Present** results to user.

#### 8.2 Final Gate Review

**Present** checklist:
```
Run Phase Checklist:
- [ ] Outcomes comparison completed
- [ ] Impact metrics documented
- [ ] Executive summary prepared
- [ ] All P1 issues resolved
- [ ] Documentation complete
- [ ] Runbook finalized
- [ ] Knowledge transfer done

Production Decision:
- [ ] Proceed to production
- [ ] Extend pilot
- [ ] Revisit approach
```

## Stopping Points

- ✋ Step 1: After project discovery
- ✋ Step 2: After team configuration
- ✋ Step 3: After infrastructure setup
- ✋ Step 4: After project plan review
- ✋ Step 5: After Foundation phase (gate review)
- ✋ Step 6: After Crawl phase (gate review)
- ✋ Step 7: After Walk phase (gate review)
- ✋ Step 8: After Run phase (final decision)

## Output

- Complete project plan with phase timelines
- Database and schema structure
- Governance controls (via `hipaa-phi-governance`)
- ML model and scoring pipeline (via `cortex-ml-classification`)
- Operational dashboard (via `operational-action-queue`)
- Phase gate checklists
- Outcomes assessment queries

## Use Case Templates

| Use Case | Target Variable | Key Features | Success Metric |
|----------|-----------------|--------------|----------------|
| No-Show | NOSHOW_FLAG | Historical rate, lead time, day of week | Slot utilization |
| Readmission | READMITTED_30D | Diagnosis, LOS, prior admits | Readmission rate |
| Denials | DENIED_FLAG | Payer, CPT, prior denial rate | Denial rate |

## Related Skills

This skill invokes:
- `hipaa-phi-governance` - Foundation phase
- `cortex-ml-classification` - Crawl phase  
- `operational-action-queue` - Walk phase
