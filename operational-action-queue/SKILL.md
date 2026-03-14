---
name: operational-action-queue
description: "Build Streamlit dashboards with human-in-the-loop action queues for operational decision-making. Use when: creating review queues for ML outputs, implementing approve/override/defer workflows, capturing user feedback, building audit trails. Triggers: action queue, approve, override, defer, human-in-the-loop, HITL, operational dashboard, review queue, workflow, feedback capture, audit log, Streamlit dashboard."
---

# Operational Action Queue

## Setup

**Query** `snowflake_product_docs` for:
- `Streamlit in Snowflake`
- `get_active_session Streamlit`
- `CREATE STREAMLIT syntax`

## Workflow

### Step 1: Use Case Discovery

**Ask** user:
```
To build your action queue dashboard:

1. What type of items are being reviewed?
   a) Appointments (scheduling)
   b) Transactions (fraud review)
   c) Claims (approval/denial)
   d) Patients (care management)
   e) Orders (fulfillment)
   f) Custom entity type: ___

2. What is the source of recommendations?
   a) ML model risk scores
   b) Business rules/flags
   c) External system alerts
   d) Manual flagging

3. Where are the items to review stored?
   (database.schema.view)
```

**⚠️ STOP**: Wait for user response.

### Step 2: Action Design

**Ask** user:
```
Action configuration:

1. What actions can users take?
   a) Approve only
   b) Approve / Reject
   c) Approve / Override / Defer
   d) Custom actions: ___

2. When overriding, should users provide a reason?
   a) Yes, required
   b) Yes, optional
   c) No

3. If using override reasons, what options?
   (Default suggestions based on use case, or provide custom list)
   - Patient called to confirm
   - Provider relationship
   - Recent pattern change
   - System data error
   - Other

4. Should deferred items reappear?
   a) Yes, next day
   b) Yes, after X days
   c) No (manual re-queue)
```

**⚠️ STOP**: Wait for user response.

### Step 3: Display Configuration

**Ask** user:
```
Display settings:

1. How should items be prioritized?
   a) By risk score (highest first)
   b) By date (oldest first)
   c) By entity/category
   d) Custom sort

2. How many items per page?
   a) 10
   b) 25
   c) 50

3. What columns to display in the queue?
   (List columns from source view)

4. What filter options?
   a) By risk tier (High/Medium/Low)
   b) By date range
   c) By status
   d) All of the above
```

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Database Objects

**Generate** SQL for supporting tables:

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

-- Action queue view (excludes deferred items)
CREATE OR REPLACE VIEW <schema>.V_ACTION_QUEUE AS
SELECT 
    q.*,
    COALESCE(a.action_type, 'PENDING') AS status,
    a.actioned_by,
    a.actioned_at
FROM <source_view> q
LEFT JOIN (
    SELECT entity_id, action_type, actioned_by, actioned_at,
           ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY actioned_at DESC) AS rn
    FROM <schema>.ACTION_LOG
) a ON q.<entity_id_col> = a.entity_id AND a.rn = 1
LEFT JOIN <schema>.DEFERRED_ITEMS d 
    ON q.<entity_id_col> = d.entity_id 
    AND d.is_active = TRUE 
    AND d.defer_until > CURRENT_DATE()
WHERE d.defer_id IS NULL
ORDER BY q.<sort_column> DESC;
```

**Generate** analytics views:
```sql
-- Action summary
CREATE OR REPLACE VIEW <schema>.V_ACTION_SUMMARY AS
SELECT 
    DATE(actioned_at) AS action_date,
    action_type,
    COUNT(*) AS action_count
FROM <schema>.ACTION_LOG
GROUP BY 1, 2;

-- Override reasons breakdown
CREATE OR REPLACE VIEW <schema>.V_OVERRIDE_REASONS AS
SELECT action_reason, COUNT(*) AS count
FROM <schema>.ACTION_LOG
WHERE action_type = 'OVERRIDE'
GROUP BY 1 ORDER BY 2 DESC;
```

**Present** SQL to user.

**⚠️ STOP**: Get approval before executing.

### Step 5: Generate Streamlit App

**Generate** Streamlit application code:

```python
# app.py
import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

st.set_page_config(page_title="<App Title>", layout="wide")
session = get_active_session()

# Sidebar filters
st.sidebar.header("Filters")
risk_filter = st.sidebar.multiselect("Risk Tier", ["HIGH", "MEDIUM", "LOW"], default=["HIGH", "MEDIUM"])
status_filter = st.sidebar.selectbox("Status", ["PENDING", "ALL"])

# Main content
st.title("<App Title>")
tab1, tab2 = st.tabs(["Action Queue", "Analytics"])

with tab1:
    st.header("Items Pending Review")
    
    # Fetch data
    risk_list = ",".join([f"'{r}'" for r in risk_filter])
    query = f"""
        SELECT * FROM <schema>.V_ACTION_QUEUE
        WHERE <risk_tier_col> IN ({risk_list})
        AND status = 'PENDING'
        LIMIT 50
    """
    df = session.sql(query).to_pandas()
    
    if df.empty:
        st.success("No items pending review!")
    else:
        for idx, row in df.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([3, 1, 2])
                
                with col1:
                    st.subheader(f"{row['<display_col>']}")
                    st.write(f"Risk: {row['<risk_score_col>']:.2f} ({row['<risk_tier_col>']})")
                
                with col2:
                    st.metric("Recommendation", row['<recommendation_col>'])
                
                with col3:
                    c1, c2, c3 = st.columns(3)
                    entity_id = row['<entity_id_col>']
                    
                    if c1.button("✓", key=f"approve_{entity_id}", help="Approve"):
                        session.sql(f"""
                            INSERT INTO <schema>.ACTION_LOG 
                            (entity_id, entity_type, action_type, original_score, actioned_by)
                            VALUES ('{entity_id}', '<entity_type>', 'APPROVE', 
                                    {row['<risk_score_col>']}, CURRENT_USER())
                        """).collect()
                        st.rerun()
                    
                    if c2.button("✎", key=f"override_{entity_id}", help="Override"):
                        st.session_state[f"show_override_{entity_id}"] = True
                    
                    if c3.button("⏸", key=f"defer_{entity_id}", help="Defer"):
                        st.session_state[f"show_defer_{entity_id}"] = True
                
                # Override form
                if st.session_state.get(f"show_override_{entity_id}"):
                    with st.form(f"override_form_{entity_id}"):
                        reason = st.selectbox("Reason", [<override_reasons>])
                        notes = st.text_area("Notes (optional)")
                        if st.form_submit_button("Submit"):
                            session.sql(f"""
                                INSERT INTO <schema>.ACTION_LOG 
                                (entity_id, entity_type, action_type, action_reason, 
                                 action_notes, original_score, actioned_by)
                                VALUES ('{entity_id}', '<entity_type>', 'OVERRIDE',
                                        '{reason}', '{notes}', {row['<risk_score_col>']}, CURRENT_USER())
                            """).collect()
                            st.session_state[f"show_override_{entity_id}"] = False
                            st.rerun()
                
                st.divider()

with tab2:
    st.header("Action Analytics")
    summary = session.sql("SELECT * FROM <schema>.V_ACTION_SUMMARY WHERE action_date >= DATEADD('day', -30, CURRENT_DATE())").to_pandas()
    st.bar_chart(summary.pivot(index='ACTION_DATE', columns='ACTION_TYPE', values='ACTION_COUNT'))
    
    st.subheader("Override Reasons")
    reasons = session.sql("SELECT * FROM <schema>.V_OVERRIDE_REASONS").to_pandas()
    st.dataframe(reasons)
```

**Present** code to user.

**⚠️ STOP**: Get approval before creating Streamlit app.

### Step 6: Deploy Streamlit App

**Execute** deployment:
```sql
CREATE OR REPLACE STREAMLIT <database>.<schema>.<app_name>
    ROOT_LOCATION = '@<stage>/streamlit/'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = '<warehouse>';
```

Or use `snow streamlit deploy` if using CLI.

### Step 7: Validation

**Validate** deployment:
```sql
-- Test action log insert
INSERT INTO <schema>.ACTION_LOG (entity_id, entity_type, action_type, actioned_by)
VALUES ('TEST', 'TEST', 'APPROVE', CURRENT_USER());
DELETE FROM <schema>.ACTION_LOG WHERE entity_id = 'TEST';

-- Verify views work
SELECT COUNT(*) FROM <schema>.V_ACTION_QUEUE;
SELECT COUNT(*) FROM <schema>.V_ACTION_SUMMARY;

-- Verify Streamlit created
SHOW STREAMLITS LIKE '<app_name>';
```

**Present** validation results and app URL to user.

## Stopping Points

- ✋ Step 1: After use case discovery
- ✋ Step 2: After action design
- ✋ Step 3: After display configuration
- ✋ Step 4: Before executing database DDL
- ✋ Step 5: Before creating Streamlit app
- ✋ Step 7: After validation (final review)

## Output

- ACTION_LOG table for audit trail
- DEFERRED_ITEMS table for defer workflow
- V_ACTION_QUEUE view for pending items
- V_ACTION_SUMMARY and V_OVERRIDE_REASONS analytics views
- Streamlit app with action queue and analytics tabs
- App URL for user access

## Related Skills

- `hipaa-phi-governance` - Dashboard should connect to Zone 1 for offshore users
- `cortex-ml-classification` - Provides the risk scores displayed in queue
- `healthcare-analytics-accelerator` - Invokes this skill during Walk phase
