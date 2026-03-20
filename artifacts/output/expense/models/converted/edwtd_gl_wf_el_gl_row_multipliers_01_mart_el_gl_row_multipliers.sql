{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_gl_row_multipliers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_GL_ROW_MULTIPLIERS',
        'target_table': 'EL_GL_ROW_MULTIPLIERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.867184+00:00'
    }
) }}

WITH 

source_st_mf_gl_row_multipliers AS (
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
    FROM {{ source('raw', 'st_mf_gl_row_multipliers') }}
),

final AS (
    SELECT
        multiplier,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        ges_update_date,
        global_name,
        create_datetime
    FROM source_st_mf_gl_row_multipliers
)

SELECT * FROM final