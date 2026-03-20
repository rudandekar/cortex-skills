{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_row_multipliers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_ROW_MULTIPLIERS',
        'target_table': 'ST_MF_GL_ROW_MULTIPLIERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.150827+00:00'
    }
) }}

WITH 

source_ff_mf_gl_row_multipliers AS (
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
    FROM {{ source('raw', 'ff_mf_gl_row_multipliers') }}
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
    FROM source_ff_mf_gl_row_multipliers
)

SELECT * FROM final