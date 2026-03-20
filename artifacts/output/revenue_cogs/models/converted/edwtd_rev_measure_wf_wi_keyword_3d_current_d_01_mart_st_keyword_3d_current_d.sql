{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_keyword_3d_current_d', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_KEYWORD_3D_CURRENT_D',
        'target_table': 'ST_KEYWORD_3D_CURRENT_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.551953+00:00'
    }
) }}

WITH 

source_st_keyword_3d_current_d AS (
    SELECT
        dw_crnt_keyword_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        problem_code,
        rpm_skillid,
        ff_skillid,
        dw_active_flag
    FROM {{ source('raw', 'st_keyword_3d_current_d') }}
),

source_wi_keyword_3d_current_d AS (
    SELECT
        dw_crnt_keyword_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        problem_code,
        rpm_skillid,
        ff_skillid,
        dw_active_flag
    FROM {{ source('raw', 'wi_keyword_3d_current_d') }}
),

final AS (
    SELECT
        dw_crnt_keyword_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        problem_code,
        rpm_skillid,
        ff_skillid,
        dw_active_flag
    FROM source_wi_keyword_3d_current_d
)

SELECT * FROM final