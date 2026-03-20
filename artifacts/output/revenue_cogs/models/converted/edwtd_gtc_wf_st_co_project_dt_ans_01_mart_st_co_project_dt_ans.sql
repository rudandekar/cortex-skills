{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_co_project_dt_ans', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_PROJECT_DT_ANS',
        'target_table': 'ST_CO_PROJECT_DT_ANS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.682953+00:00'
    }
) }}

WITH 

source_ff_co_project_dt_ans_incr AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_co_project_dt_ans_incr') }}
),

final AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code
    FROM source_ff_co_project_dt_ans_incr
)

SELECT * FROM final