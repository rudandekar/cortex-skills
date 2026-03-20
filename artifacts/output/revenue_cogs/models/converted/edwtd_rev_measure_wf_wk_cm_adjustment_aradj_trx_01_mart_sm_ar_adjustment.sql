{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_cm_adjustment_aradj_trx', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WK_CM_ADJUSTMENT_ARADJ_TRX',
        'target_table': 'SM_AR_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.698843+00:00'
    }
) }}

WITH 

source_n_ar_adjustment_trx AS (
    SELECT
        bk_ar_adj_number,
        bk_company_code,
        bk_set_of_books_key,
        ar_adj_trx_reason_code,
        gl_date,
        gl_posted_date,
        adjustment_source_type_code,
        transaction_datetime,
        ar_adj_trx_functional_amt,
        ar_adj_trx_transactional_amt,
        bk_ar_adj_type_code,
        ar_trx_key,
        sk_adjustment_id_int,
        ss_source_application_code,
        ss_code,
        general_ledger_account_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        ar_adj_trx_item_usd_amt,
        ar_adj_trx_item_trxl_amt,
        ar_adj_trx_freight_usd_amt,
        ar_adj_trx_freight_trxl_amt,
        ar_adj_trx_tax_usd_amt,
        ar_adj_trx_tax_trxn_amt,
        ar_type_cd,
        ar_adj_trx_usd_amt,
        ar_adjustment_status_cd,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM {{ source('raw', 'n_ar_adjustment_trx') }}
),

final AS (
    SELECT
        ar_adjustment_key,
        bk_ar_adj_number,
        bk_company_code,
        bk_set_of_books_key,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM source_n_ar_adjustment_trx
)

SELECT * FROM final