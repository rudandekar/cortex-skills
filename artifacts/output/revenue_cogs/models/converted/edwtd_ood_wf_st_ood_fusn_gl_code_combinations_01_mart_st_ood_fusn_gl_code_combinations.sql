{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_gl_code_combinations', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_GL_CODE_COMBINATIONS',
        'target_table': 'ST_OOD_FUSN_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.769202+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_gl_code_combinations AS (
    SELECT
        code_combination_id,
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
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_gl_code_combinations') }}
),

final AS (
    SELECT
        code_combination_id,
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
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_gl_code_combinations
)

SELECT * FROM final