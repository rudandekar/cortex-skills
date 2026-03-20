{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_co_dt_steps', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_DT_STEPS',
        'target_table': 'FF_CO_DT_STEPS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.474153+00:00'
    }
) }}

WITH 

source_co_dt_steps AS (
    SELECT
        step_id,
        type,
        type_seq,
        step_desc,
        rule,
        status,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        step_dtl
    FROM {{ source('raw', 'co_dt_steps') }}
),

transformed_exptrans AS (
    SELECT
    step_id,
    type,
    type_seq,
    step_desc,
    rule,
    status,
    created_by,
    created_date,
    last_updated_by,
    last_updated_date,
    step_dtl,
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_dt_steps
),

final AS (
    SELECT
        batch_id,
        step_id,
        type_1,
        type_seq,
        step_desc,
        rule,
        status,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        step_dtl,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final