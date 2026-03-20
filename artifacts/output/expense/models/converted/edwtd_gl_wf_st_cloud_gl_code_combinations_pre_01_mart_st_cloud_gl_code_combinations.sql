{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_gl_code_combinations_pre', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_GL_CODE_COMBINATIONS_PRE',
        'target_table': 'ST_CLOUD_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.025407+00:00'
    }
) }}

WITH 

source_pst_cloud_gl_code_combinations AS (
    SELECT
        account_type,
        alternate_code_combination_id,
        company_cost_center_org_id,
        creation_date,
        detail_posting_allowed_flag,
        enabled_flag,
        end_date_active,
        financial_category,
        code_combination_id,
        last_update_date,
        ledger_segment,
        ledger_type_code,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        segment8,
        start_date_active,
        summary_flag,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'pst_cloud_gl_code_combinations') }}
),

final AS (
    SELECT
        code_combination_id,
        last_update_date,
        last_updated_by,
        chart_of_accounts_id,
        detail_posting_allowed_flag,
        detail_budgeting_allowed_flag,
        account_type,
        enabled_flag,
        summary_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        preserve_flag,
        company_cost_center_org_id,
        revaluation_id,
        ledger_segment,
        ledger_type_code,
        alternate_code_combination_id,
        create_datetime,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_gl_code_combinations
)

SELECT * FROM final