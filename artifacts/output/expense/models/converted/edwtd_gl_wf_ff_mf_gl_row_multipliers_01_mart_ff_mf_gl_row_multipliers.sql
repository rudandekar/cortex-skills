{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_gl_row_multipliers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_GL_ROW_MULTIPLIERS',
        'target_table': 'FF_MF_GL_ROW_MULTIPLIERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.611733+00:00'
    }
) }}

WITH 

source_mf_gl_row_multipliers AS (
    SELECT
        multiplier,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'mf_gl_row_multipliers') }}
),

transformed_exp_mf_gl_row_multipliers AS (
    SELECT
    multiplier,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    ges_update_date,
    global_name,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_gl_row_multipliers
),

final AS (
    SELECT
        batch_id,
        multiplier,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM transformed_exp_mf_gl_row_multipliers
)

SELECT * FROM final