{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_xla_ae_lines', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_XLA_AE_LINES',
        'target_table': 'PST_CLOUD_XLA_AE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.707132+00:00'
    }
) }}

WITH 

source_cbm_xla_ae_lines AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        gl_sl_link_id,
        accounting_class_code,
        entered_dr,
        entered_cr,
        accounted_dr,
        accounted_cr,
        currency_code,
        gl_sl_link_table,
        accounting_date,
        ledger_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_xla_ae_lines') }}
),

final AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        gl_sl_link_id,
        accounting_class_code,
        entered_dr,
        entered_cr,
        accounted_dr,
        accounted_cr,
        currency_code,
        gl_sl_link_table,
        accounting_date,
        ledger_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_xla_ae_lines
)

SELECT * FROM final