{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sales_account_rstd_sls_trx', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_MT_SALES_ACCOUNT_RSTD_SLS_TRX',
        'target_table': 'MT_SALES_ACCOUNT_RSTD_SLS_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.908365+00:00'
    }
) }}

WITH 

source_mt_sales_account_rstd_sls_trx AS (
    SELECT
        sales_order_line_key,
        bk_sa_member_id_int,
        dv_end_cust_party_key,
        sales_account_group_party_key,
        dv_end_cust_ownership_splt_pct,
        sales_territory_key,
        sales_rep_num,
        transaction_type,
        bookings_measure_key,
        dv_sav_reason_descr,
        dv_end_cust_reason_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_pos_transaction_id_int,
        so_sbscrptn_itm_sls_trx_key
    FROM {{ source('raw', 'mt_sales_account_rstd_sls_trx') }}
),

final AS (
    SELECT
        sales_order_line_key,
        bk_sa_member_id_int,
        dv_end_cust_party_key,
        sales_account_group_party_key,
        dv_end_cust_ownership_splt_pct,
        sales_territory_key,
        sales_rep_num,
        transaction_type,
        bookings_measure_key,
        dv_sav_reason_descr,
        dv_end_cust_reason_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_pos_transaction_id_int,
        so_sbscrptn_itm_sls_trx_key
    FROM source_mt_sales_account_rstd_sls_trx
)

SELECT * FROM final