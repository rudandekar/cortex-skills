{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_2tier_invoice', 'batch', 'edwtd_2tier'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_2TIER_INVOICE',
        'target_table': 'N_AR_TRX_2TIER_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.489501+00:00'
    }
) }}

WITH 

source_w_ar_trx_2tier_invoice AS (
    SELECT
        ar_trx_2tier_invoice_key,
        two_tier_invoice_usd_amt,
        two_tier_invoice_trxl_amt,
        ar_transaction_key,
        fiscal_calendar_cd,
        fiscal_year_num_int,
        fiscal_month_num_int,
        dd_two_tier_theater_name,
        dd_erp_sales_channel_cd,
        dd_operating_unit_name_cd,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_2tier_invoice') }}
),

final AS (
    SELECT
        ar_trx_2tier_invoice_key,
        two_tier_invoice_usd_amt,
        two_tier_invoice_trxl_amt,
        ar_transaction_key,
        fiscal_calendar_cd,
        fiscal_year_num_int,
        fiscal_month_num_int,
        dd_two_tier_theater_name,
        dd_erp_sales_channel_cd,
        dd_operating_unit_name_cd,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rol_trx_line_gl_distri_key
    FROM source_w_ar_trx_2tier_invoice
)

SELECT * FROM final