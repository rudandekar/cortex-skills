{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_OOD',
        'target_table': 'N_AR_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.518138+00:00'
    }
) }}

WITH 

source_w_ar_trx AS (
    SELECT
        ar_trx_key,
        bk_ar_trx_number,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_class_type,
        ar_trx_datetime,
        ar_trx_reason_code,
        ar_trx_complete_flag,
        bk_trxl_currency_code,
        ru_cust_purchase_order_number,
        ru_ar_cmdm_adjustment_type,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        ru_bk_adjd_ar_trx_key,
        sk_customer_trx_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        interface_source_name,
        sales_order_key,
        bk_ar_payment_term_name,
        ar_batch_source_key,
        dv_ar_trx_dt,
        manual_transaction_flg,
        ood_trx_role,
        ru_trxnl_to_usd_conversion_rt,
        ru_trxnl_to_usd_conversion_dt,
        bk_deal_id,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx') }}
),

final AS (
    SELECT
        ar_trx_key,
        bk_ar_trx_number,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_class_type,
        ar_trx_datetime,
        ar_trx_reason_code,
        ar_trx_complete_flag,
        bk_trxl_currency_code,
        ru_cust_purchase_order_number,
        ru_ar_cmdm_adjustment_type,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        ru_bk_adjd_ar_trx_key,
        sk_customer_trx_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        interface_source_name,
        sales_order_key,
        bk_ar_payment_term_name,
        ar_batch_source_key,
        dv_ar_trx_dt,
        manual_transaction_flg,
        ood_trx_role,
        ru_trxnl_to_usd_conversion_rt,
        ru_trxnl_to_usd_conversion_dt,
        bk_deal_id
    FROM source_w_ar_trx
)

SELECT * FROM final