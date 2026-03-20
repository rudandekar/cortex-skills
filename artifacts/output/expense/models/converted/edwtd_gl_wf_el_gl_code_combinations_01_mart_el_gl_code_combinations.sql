{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_gl_code_combinations', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_GL_CODE_COMBINATIONS',
        'target_table': 'EL_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.062876+00:00'
    }
) }}

WITH 

source_st_mf_gl_code_combinations AS (
    SELECT
        batch_id,
        code_combination_id,
        global_name,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        ges_update_date,
        create_datetime,
        action_code,
        last_update_date
    FROM {{ source('raw', 'st_mf_gl_code_combinations') }}
),

final AS (
    SELECT
        code_combination_id,
        global_name,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        chart_of_accounts_id,
        enabled_flag,
        start_date_active,
        end_date_active,
        account_type,
        preserve_flag,
        create_datetime,
        last_update_date
    FROM source_st_mf_gl_code_combinations
)

SELECT * FROM final