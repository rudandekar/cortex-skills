{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_co_dt_steps', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_DT_STEPS',
        'target_table': 'ST_CO_DT_STEPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.687892+00:00'
    }
) }}

WITH 

source_ff_co_dt_steps_incr AS (
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
    FROM {{ source('raw', 'ff_co_dt_steps_incr') }}
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
    FROM source_ff_co_dt_steps_incr
)

SELECT * FROM final