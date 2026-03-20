{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_gl_code_combinations', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_GL_CODE_COMBINATIONS',
        'target_table': 'PST_CLOUD_GL_CODE_COMBINATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.942255+00:00'
    }
) }}

WITH 

source_cbm_gl_code_combinations AS (
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
    FROM {{ source('raw', 'cbm_gl_code_combinations') }}
),

final AS (
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
    FROM source_cbm_gl_code_combinations
)

SELECT * FROM final