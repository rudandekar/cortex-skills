{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_xla_ae_lines', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_XLA_AE_LINES',
        'target_table': 'ST_MF_XLA_AE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.696163+00:00'
    }
) }}

WITH 

source_ff_mf_xla_ae_lines AS (
    SELECT
        batch_id,
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        accounting_class_code,
        currency_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        accounting_date,
        ledger_id,
        gl_transfer_mode_code,
        gl_sl_link_id,
        gl_sl_link_table,
        displayed_line_number,
        business_class_code,
        source_table,
        source_id,
        description,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_xla_ae_lines') }}
),

final AS (
    SELECT
        batch_id,
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        accounting_class_code,
        currency_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        accounting_date,
        ledger_id,
        gl_transfer_mode_code,
        gl_sl_link_id,
        gl_sl_link_table,
        displayed_line_number,
        business_class_code,
        source_table,
        source_id,
        ges_update_date,
        description,
        global_name,
        create_datetime,
        action_code
    FROM source_ff_mf_xla_ae_lines
)

SELECT * FROM final