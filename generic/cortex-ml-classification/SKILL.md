---
name: cortex-ml-classification
description: "Build, train, evaluate, and deploy classification models using Snowflake Cortex ML CLASSIFICATION. Use when: predicting binary/multi-class outcomes like no-show, churn, fraud, default risk, conversion propensity. Triggers: classification, predict, no-show, churn, fraud, propensity, risk score, binary classification, multi-class, Cortex ML, ML model, train model, scoring pipeline."
---

# Cortex ML Classification

## Setup

**Query** `snowflake_product_docs` for: `Cortex ML CLASSIFICATION`, `CLASSIFICATION CREATE`, `CLASSIFICATION PREDICT`, `SHOW_EVALUATION_METRICS`, `SHOW_FEATURE_IMPORTANCE`.

## Workflow

### Step 1: Use Case Discovery

**Ask** user: What to predict (no-show/churn/fraud/default/conversion/custom), whether source system already provides a score, and binary vs multi-class.

**⚠️ STOP**: Wait for user response.

### Step 2: Data Discovery

**Ask** user: Historical labeled data location (database.schema.table), target column name, columns to exclude (IDs, PHI, leaky features), and approximate row count.

**⚠️ STOP**: Wait for user response.

### Step 3: Feature Engineering

Analyze source table with DESCRIBE TABLE and class distribution query. **Ask** user about: historical rates (e.g., past no-show rate), time-based features (day of week, lead time, season), and high-cardinality categorical columns.

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Training Data

Generate training data preparation SQL:

```sql
CREATE OR REPLACE TABLE <schema>.TRAINING_DATA_<use_case> AS
SELECT 
    t.<target> AS label,
    t.<feature_cols>,
    DAYOFWEEK(t.<date_column>) AS day_of_week,
    DATEDIFF('day', t.<created_date>, t.<event_date>) AS lead_time_days,
    COALESCE(h.historical_rate, 0) AS historical_rate
FROM <source_table> t
LEFT JOIN historical_rates h ON t.<entity_id> = h.<entity_id>
WHERE t.<date_column> BETWEEN '<train_start>' AND '<train_end>';
```

Create train/test split using SAMPLE (80/20).

**⚠️ STOP**: Get approval before executing.

### Step 5: Train Model

```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION <schema>.<model_name>(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '<schema>.TRAIN_SET'),
    TARGET_COLNAME => 'LABEL',
    CONFIG_OBJECT => {'evaluate': TRUE, 'on_error': 'skip'}
);

CALL <schema>.<model_name>!SHOW_EVALUATION_METRICS();
CALL <schema>.<model_name>!SHOW_FEATURE_IMPORTANCE();
CALL <schema>.<model_name>!SHOW_CONFUSION_MATRIX();
```

Present accuracy, precision, recall, AUC, and top features. Ask user to accept, reject (retrain), or review confusion matrix.

**⚠️ STOP**: Wait for user decision.

### Step 6: Deployment Configuration

**Ask** user: Batch vs real-time scoring, schedule for batch, and probability threshold (0.5 balanced / 0.3 high recall / 0.7 high precision / custom).

**⚠️ STOP**: Wait for user response.

### Step 7: Generate Scoring Pipeline

```sql
CREATE OR REPLACE VIEW <schema>.V_<use_case>_SCORES AS
SELECT 
    <entity_id>,
    <event_id>,
    <schema>.<model_name>!PREDICT(OBJECT_CONSTRUCT(*)):class::INT AS predicted_class,
    <schema>.<model_name>!PREDICT(OBJECT_CONSTRUCT(*)):probability:1::FLOAT AS probability,
    CASE 
        WHEN probability >= <high_threshold> THEN 'HIGH'
        WHEN probability >= <med_threshold> THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier
FROM <scoring_source>;
```

Generate score history fact table, batch scoring task with configured schedule, and drift monitoring view. Present SQL and execute upon approval.

### Step 8: Validation

Verify model exists, test prediction with LIMIT 1, and verify scoring task is scheduled.

## Stopping Points

- ✋ Steps 1-3: After each discovery step
- ✋ Step 4: Before executing training data SQL
- ✋ Step 5: After model evaluation (accept/reject)
- ✋ Step 6: After deployment configuration
- ✋ Step 7: Before executing scoring pipeline SQL

## Output

- Training data table with engineered features
- Trained CLASSIFICATION model with evaluation metrics
- Scoring view, score history table, scheduled task, drift monitoring view

## Related Skills

- `hipaa-phi-governance` - Ensure training data uses Zone 1 if offshore
- `operational-action-queue` - Display scores in HITL dashboard
- `healthcare-analytics-accelerator` - Invokes this skill during Crawl phase
