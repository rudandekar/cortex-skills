---
name: cortex-ml-classification
description: "Build, train, evaluate, and deploy classification models using Snowflake Cortex ML CLASSIFICATION. Use when: predicting binary/multi-class outcomes like no-show, churn, fraud, default risk, conversion propensity. Triggers: classification, predict, no-show, churn, fraud, propensity, risk score, binary classification, multi-class, Cortex ML, ML model, train model, scoring pipeline."
---

# Cortex ML Classification

## Setup

**Query** `snowflake_product_docs` for:
- `Cortex ML CLASSIFICATION`
- `SNOWFLAKE.ML.CLASSIFICATION CREATE`
- `CLASSIFICATION PREDICT function`
- `SHOW_EVALUATION_METRICS`
- `SHOW_FEATURE_IMPORTANCE`

## Workflow

### Step 1: Use Case Discovery

**Ask** user:
```
To build your classification model:

1. What are you trying to predict?
   a) No-show (appointment attendance)
   b) Churn (customer leaving)
   c) Fraud (suspicious activity)
   d) Default (loan/payment risk)
   e) Conversion (purchase propensity)
   f) Custom outcome

2. Does the source system already provide a prediction/score?
   a) No - build from scratch
   b) Yes - but want to compare/improve
   c) Yes - use as additional feature

3. Is this binary (2 outcomes) or multi-class (3+)?
   a) Binary (yes/no, 0/1)
   b) Multi-class (multiple categories)
```

**⚠️ STOP**: Wait for user response.

### Step 2: Data Discovery

**Ask** user:
```
Data configuration:

1. Where is your historical labeled data?
   (database.schema.table with past outcomes)

2. What is the target column (outcome to predict)?
   (column name containing 0/1 or category)

3. What columns should be EXCLUDED from features?
   (IDs, PHI columns, leaky features that wouldn't be known at prediction time)

4. Approximately how many rows of historical data?
   a) < 10,000
   b) 10,000 - 100,000
   c) 100,000 - 1,000,000
   d) > 1,000,000
```

**⚠️ STOP**: Wait for user response.

### Step 3: Feature Engineering

**Analyze** the source table:
```sql
DESCRIBE TABLE <source_table>;
SELECT COUNT(*), COUNT(DISTINCT <target>) FROM <source_table>;
```

**Ask** user:
```
Feature engineering options:

1. Include historical rates? (e.g., past no-show rate per patient)
   - Yes / No

2. Include time-based features?
   - Day of week
   - Hour of day
   - Lead time (days until event)
   - Month/Season
   - All of the above
   - None

3. Any categorical columns needing special handling?
   (Cortex auto-encodes, but list high-cardinality columns)
```

**⚠️ STOP**: Wait for user response.

### Step 4: Generate Training Data

**Generate** training data preparation SQL:

```sql
CREATE OR REPLACE TABLE <schema>.TRAINING_DATA_<use_case> AS
WITH historical_rates AS (
    SELECT 
        <entity_id>,
        COUNT(*) AS total_events,
        SUM(CASE WHEN <target> = 1 THEN 1 ELSE 0 END) AS positive_events,
        ROUND(positive_events / NULLIF(total_events, 0), 3) AS historical_rate
    FROM <source_table>
    WHERE <date_column> < DATEADD('month', -1, CURRENT_DATE())
    GROUP BY <entity_id>
)
SELECT 
    t.<target> AS label,
    -- Categorical features
    t.<cat_col_1>,
    t.<cat_col_2>,
    -- Numeric features  
    t.<num_col_1>,
    -- Time features
    DAYOFWEEK(t.<date_column>) AS day_of_week,
    DATEDIFF('day', t.<created_date>, t.<event_date>) AS lead_time_days,
    -- Historical behavior
    COALESCE(h.historical_rate, 0) AS historical_rate
FROM <source_table> t
LEFT JOIN historical_rates h ON t.<entity_id> = h.<entity_id>
WHERE t.<date_column> BETWEEN '<train_start>' AND '<train_end>';

-- Train/Test split
CREATE OR REPLACE TABLE <schema>.TRAIN_SET AS
SELECT * FROM <schema>.TRAINING_DATA_<use_case> SAMPLE (80);

CREATE OR REPLACE TABLE <schema>.TEST_SET AS
SELECT * FROM <schema>.TRAINING_DATA_<use_case>
MINUS
SELECT * FROM <schema>.TRAIN_SET;
```

**Present** SQL to user for review.

**⚠️ STOP**: Get approval before executing.

### Step 5: Train Model

**Execute** training:
```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION <schema>.<model_name>(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', '<schema>.TRAIN_SET'),
    TARGET_COLNAME => 'LABEL',
    CONFIG_OBJECT => {'evaluate': TRUE, 'on_error': 'skip'}
);
```

**After training completes**, retrieve evaluation:
```sql
CALL <schema>.<model_name>!SHOW_EVALUATION_METRICS();
CALL <schema>.<model_name>!SHOW_FEATURE_IMPORTANCE();
CALL <schema>.<model_name>!SHOW_CONFUSION_MATRIX();
```

**Present** results to user:
```
Model Training Complete!

Evaluation Metrics:
- Accuracy: XX%
- Precision: XX%
- Recall: XX%
- AUC: X.XX

Top 5 Features:
1. <feature> (importance: X.XX)
2. <feature> (importance: X.XX)
...

Is this performance acceptable?
- [Yes] Proceed to deployment
- [No] Adjust features and retrain
- [Review] Show confusion matrix
```

**⚠️ STOP**: Wait for user decision.

### Step 6: Deployment Configuration

**Ask** user:
```
Deployment options:

1. How should the model be used?
   a) Batch scoring (scheduled daily/hourly)
   b) Real-time scoring (on-demand)
   c) Both

2. For batch scoring, what schedule?
   a) Daily at 6 AM
   b) Hourly
   c) Custom cron expression

3. What probability threshold for positive class?
   a) 0.5 (balanced)
   b) 0.3 (high recall - catch more positives)
   c) 0.7 (high precision - fewer false positives)
   d) Custom
```

**⚠️ STOP**: Wait for user response.

### Step 7: Generate Scoring Pipeline

**Generate** scoring artifacts:

```sql
-- Scoring view
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

-- Score history table
CREATE OR REPLACE TABLE <schema>.FACT_<use_case>_SCORES (
    <entity_id> VARCHAR,
    <event_id> VARCHAR,
    score_date DATE,
    predicted_class INT,
    probability FLOAT,
    risk_tier VARCHAR,
    model_version VARCHAR,
    scored_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Batch scoring task
CREATE OR REPLACE TASK <schema>.TASK_SCORE_<use_case>
    WAREHOUSE = <warehouse>
    SCHEDULE = '<cron_schedule>'
AS
INSERT INTO <schema>.FACT_<use_case>_SCORES
SELECT <entity_id>, <event_id>, CURRENT_DATE(), 
       predicted_class, probability, risk_tier, '<model_name>_v1'
FROM <schema>.V_<use_case>_SCORES
WHERE <event_date> >= CURRENT_DATE();

ALTER TASK <schema>.TASK_SCORE_<use_case> RESUME;
```

**Generate** monitoring view:
```sql
CREATE OR REPLACE VIEW <schema>.V_MODEL_DRIFT AS
SELECT 
    score_date,
    COUNT(*) AS total_scored,
    AVG(probability) AS avg_probability,
    SUM(CASE WHEN risk_tier = 'HIGH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS high_risk_pct,
    LAG(high_risk_pct, 7) OVER (ORDER BY score_date) AS high_risk_pct_7d_ago
FROM <schema>.FACT_<use_case>_SCORES
GROUP BY score_date;
```

**Present** generated SQL and execute upon approval.

### Step 8: Validation

**Run** validation queries:
```sql
-- Verify model exists
SHOW SNOWFLAKE.ML.CLASSIFICATION LIKE '<model_name>' IN SCHEMA <schema>;

-- Test prediction
SELECT <schema>.<model_name>!PREDICT(OBJECT_CONSTRUCT(*)) 
FROM <test_table> LIMIT 1;

-- Verify task scheduled
SHOW TASKS LIKE 'TASK_SCORE_<use_case>' IN SCHEMA <schema>;
```

**Present** validation results.

## Stopping Points

- ✋ Step 1: After use case discovery
- ✋ Step 2: After data discovery
- ✋ Step 3: After feature engineering choices
- ✋ Step 4: Before executing training data SQL
- ✋ Step 5: After model evaluation (accept/reject)
- ✋ Step 6: After deployment configuration
- ✋ Step 7: Before executing scoring pipeline SQL

## Output

- Training data table with engineered features
- Trained CLASSIFICATION model
- Evaluation metrics (accuracy, precision, recall, AUC)
- Feature importance ranking
- Scoring view for real-time predictions
- Score history fact table
- Scheduled scoring task
- Drift monitoring view

## Related Skills

- `hipaa-phi-governance` - Ensure training data uses Zone 1 (masked) if offshore team involved
- `operational-action-queue` - Display scores in human-in-the-loop dashboard
- `healthcare-analytics-accelerator` - Invokes this skill during Crawl phase
