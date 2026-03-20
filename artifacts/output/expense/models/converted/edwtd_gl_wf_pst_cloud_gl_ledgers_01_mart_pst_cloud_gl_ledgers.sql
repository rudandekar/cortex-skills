{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_gl_ledgers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_GL_LEDGERS',
        'target_table': 'PST_CLOUD_GL_LEDGERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.100827+00:00'
    }
) }}

WITH 

source_cbm_gl_ledgers AS (
    SELECT
        accounting_method_code,
        accounting_method_type_code,
        acct_method_name,
        calendar_start_date,
        period_type,
        key_flex_name,
        accounted_period_type,
        allow_intercompany_post_flag,
        bal_seg_column_name,
        bal_seg_value_option_code,
        chart_of_accounts_id,
        complete_flag,
        completion_status_code,
        created_by,
        creation_date,
        currency_code,
        description,
        ledger_id,
        last_update_date,
        last_updated_by,
        ledger_category_code,
        name,
        period_set_name,
        short_name,
        sla_accounting_method_code,
        sla_accounting_method_type,
        suspense_allowed_flag,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_gl_ledgers') }}
),

final AS (
    SELECT
        accounting_method_code,
        accounting_method_type_code,
        acct_method_name,
        calendar_start_date,
        period_type,
        key_flex_name,
        accounted_period_type,
        allow_intercompany_post_flag,
        bal_seg_column_name,
        bal_seg_value_option_code,
        chart_of_accounts_id,
        complete_flag,
        completion_status_code,
        created_by,
        creation_date,
        currency_code,
        description,
        ledger_id,
        last_update_date,
        last_updated_by,
        ledger_category_code,
        name,
        period_set_name,
        short_name,
        sla_accounting_method_code,
        sla_accounting_method_type,
        suspense_allowed_flag,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_gl_ledgers
)

SELECT * FROM final