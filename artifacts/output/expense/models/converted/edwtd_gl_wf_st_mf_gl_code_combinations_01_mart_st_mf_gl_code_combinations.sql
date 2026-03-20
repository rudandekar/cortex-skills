{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_code_combinations', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_CODE_COMBINATIONS',
        'target_table': 'ST_MF_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.984148+00:00'
    }
) }}

WITH 

source_ff_mf_gl_code_combinations AS (
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
    FROM {{ source('raw', 'ff_mf_gl_code_combinations') }}
),

final AS (
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
    FROM source_ff_mf_gl_code_combinations
)

SELECT * FROM final