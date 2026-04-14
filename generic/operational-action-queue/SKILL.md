---
name: operational-action-queue
description: "Build Streamlit dashboards with human-in-the-loop action queues for operational decision-making. Use when: creating review queues for ML outputs, implementing approve/override/defer workflows, capturing user feedback, building audit trails. Triggers: action queue, approve, override, defer, human-in-the-loop, HITL, operational dashboard, review queue, workflow, feedback capture, audit log, Streamlit dashboard."
---

# Operational Action Queue

## Setup

**Query** `snowflake_product_docs` for: `Streamlit in Snowflake`, `get_active_session`, `CREATE STREAMLIT syntax`.

## Workflow

### Step 1: Use Case Discovery

**Ask** user: Entity type being reviewed (appointments/transactions/claims/patients/orders/custom), source of recommendations (ML scores / business rules / external alerts / manual flags), and source view location (database.schema.view).

**⚠️ STOP**: Wait for user response.

### Step 2: Action Design

**Ask** user: Available actions (approve only / approve+reject / approve+override+defer / custom), whether override requires a reason (required/optional/no), override reason options (e.g., patient called, provider relationship, data error), and deferred item reappearance policy.

**⚠️ STOP**: Wait for user response.

### Step 3: Display Configuration

**Ask** user: Prioritization (risk score / date / category / custom), items per page (10/25/50), columns to display, and filter options (risk tier / date range / status).

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Database Objects

```sql
-- Action log (audit trail)
CREATE OR REPLACE TABLE <schema>.ACTION_LOG (
    action_id VARCHAR DEFAULT UUID_STRING(),
    entity_id VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    action_type VARCHAR NOT NULL,
    action_reason VARCHAR,
    action_notes TEXT,
    original_recommendation VARCHAR,
    original_score FLOAT,
    actioned_by VARCHAR NOT NULL,
    actioned_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (action_id)
);

-- Deferred items
CREATE OR REPLACE TABLE <schema>.DEFERRED_ITEMS (
    defer_id VARCHAR DEFAULT UUID_STRING(),
    entity_id VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    deferred_by VARCHAR NOT NULL,
    deferred_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    defer_until DATE NOT NULL,
    defer_reason VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (defer_id)
);
```

Create V_ACTION_QUEUE view joining source with ACTION_LOG (latest action per entity) and excluding active deferred items.

Create V_ACTION_SUMMARY (daily action counts by type) and V_OVERRIDE_REASONS (override reason breakdown) analytics views.

**⚠️ STOP**: Get approval before executing.

### Step 5: Generate Streamlit App

Generate a Streamlit app with two tabs: Action Queue (filterable items with approve/override/defer buttons, override reason form) and Analytics (action summary bar chart, override reasons table).

**Load** references/streamlit-template.py for the full Streamlit app template.

**⚠️ STOP**: Get approval before creating Streamlit app.

### Step 6: Deploy Streamlit App

```sql
CREATE OR REPLACE STREAMLIT <database>.<schema>.<app_name>
    ROOT_LOCATION = '@<stage>/streamlit/'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = '<warehouse>';
```

### Step 7: Validation

Test action log insert/delete, verify views return data, verify Streamlit app is created. Present app URL.

## Stopping Points

- ✋ Steps 1-3: After each discovery step
- ✋ Step 4: Before executing database DDL
- ✋ Step 5: Before creating Streamlit app
- ✋ Step 7: After validation (final review)

## Output

- ACTION_LOG table for audit trail
- DEFERRED_ITEMS table for defer workflow
- V_ACTION_QUEUE, V_ACTION_SUMMARY, V_OVERRIDE_REASONS views
- Streamlit app with action queue and analytics tabs

## Related Skills

- `hipaa-phi-governance` - Dashboard should connect to Zone 1 for offshore users
- `cortex-ml-classification` - Provides the risk scores displayed in queue
- `healthcare-analytics-accelerator` - Invokes this skill during Walk phase
