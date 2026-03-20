{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_tax_line_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_TAX_LINE_OOD',
        'target_table': 'N_AR_TRX_TAX_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.964313+00:00'
    }
) }}

WITH 

source_w_ar_trx_tax_line AS (
    SELECT
        ar_trx_line_key,
        ru_tax_credit_amount,
        ru_tax_debit_amount,
        tax_type_code,
        tax_rt_percentage,
        tax_line_cr_dr_type,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        related_ar_trx_line_type,
        ru_ar_trx_item_line_key,
        ru_ar_trx_freight_line_key,
        ss_code,
        sk_customer_trx_line_id_lint,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_tax_line') }}
),

final AS (
    SELECT
        ar_trx_line_key,
        ru_tax_credit_amount,
        ru_tax_debit_amount,
        tax_type_code,
        tax_rt_percentage,
        tax_line_cr_dr_type,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        related_ar_trx_line_type,
        ru_ar_trx_item_line_key,
        ru_ar_trx_freight_line_key,
        ss_code,
        sk_customer_trx_line_id_lint
    FROM source_w_ar_trx_tax_line
)

SELECT * FROM final