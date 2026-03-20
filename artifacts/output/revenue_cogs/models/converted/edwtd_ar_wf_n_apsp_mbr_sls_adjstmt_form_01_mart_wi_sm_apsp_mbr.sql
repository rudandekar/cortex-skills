{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_mbr_sls_adjstmt_form', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_APSP_MBR_SLS_ADJSTMT_FORM',
        'target_table': 'WI_SM_APSP_MBR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.185033+00:00'
    }
) }}

WITH 

source_n_apsp_mbr_sls_adjstmt_form AS (
    SELECT
        apsp_mbr_sls_adjstmt_form_key,
        dv_saf_usd_amt,
        sk_cust_trx_id_lint,
        sales_order_key,
        purchase_order_key,
        sales_territory_key,
        erp_cust_acct_loc_use_key,
        item_key,
        sk_saf_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_apsp_mbr_sls_adjstmt_form') }}
),

source_wi_sm_apsp_mbr AS (
    SELECT
        apsp_mbr_sls_adjstmt_form_key,
        dv_saf_usd_amt,
        sk_cust_trx_id_lint,
        sales_order_key,
        purchase_order_key,
        sales_territory_key,
        erp_cust_acct_loc_use_key,
        item_key,
        sk_saf_id_int,
        saf_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_sm_apsp_mbr') }}
),

final AS (
    SELECT
        apsp_mbr_sls_adjstmt_form_key,
        dv_saf_usd_amt,
        sk_cust_trx_id_lint,
        sales_order_key,
        purchase_order_key,
        sales_territory_key,
        erp_cust_acct_loc_use_key,
        item_key,
        sk_saf_id_int,
        saf_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_wi_sm_apsp_mbr
)

SELECT * FROM final