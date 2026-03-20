{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_co_project_dt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_PROJECT_DT',
        'target_table': 'FF_CO_PROJECT_DT_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.331484+00:00'
    }
) }}

WITH 

source_co_project_dt AS (
    SELECT
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id
    FROM {{ source('raw', 'co_project_dt') }}
),

transformed_exptrans AS (
    SELECT
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
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_project_dt
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
    FROM transformed_exptrans
)

SELECT * FROM final