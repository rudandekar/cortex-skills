{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rbk_cma_enrch_pos_trx_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_RBK_CMA_ENRCH_POS_TRX_LN',
        'target_table': 'W_RBK_CMA_ENRCH_POS_TRX_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.838674+00:00'
    }
) }}

WITH 

source_ex_cma_trx_prts_ext_pos AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id,
        exception_type
    FROM {{ source('raw', 'ex_cma_trx_prts_ext_pos') }}
),

source_st_cma_trx_prts_ext_pos AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_cma_trx_prts_ext_pos') }}
),

source_wi_rbk_cma_enrch_pos_trx_ln AS (
    SELECT
        bk_pos_transaction_id_int,
        ship_to_cust_party_key,
        bill_to_cust_party_key,
        sold_to_cust_party_key,
        end_user_theater_name,
        ship_to_theater_name,
        pos_trx_type_cd,
        three_tier_disti_ord_trxtyp_cd,
        src_rptd_shp_cust_cr_gu_id_int,
        src_rptd_bil_cust_cr_gu_id_int,
        src_rptd_sld_cust_cr_gu_id_int,
        src_rptd_end_cust_cr_gu_id_int,
        end_customer_party_key,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'wi_rbk_cma_enrch_pos_trx_ln') }}
),

source_w_rbk_cma_enrch_pos_trx_ln AS (
    SELECT
        bk_pos_transaction_id_int,
        ship_to_cust_party_key,
        bill_to_cust_party_key,
        sold_to_cust_party_key,
        end_user_theater_name,
        ship_to_theater_name,
        pos_trx_type_cd,
        three_tier_disti_ord_trxtyp_cd,
        src_rptd_shp_cust_cr_gu_id_int,
        src_rptd_bil_cust_cr_gu_id_int,
        src_rptd_sld_cust_cr_gu_id_int,
        src_rptd_end_cust_cr_gu_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_customer_party_key,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rbk_cma_enrch_pos_trx_ln') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        ship_to_cust_party_key,
        bill_to_cust_party_key,
        sold_to_cust_party_key,
        end_user_theater_name,
        ship_to_theater_name,
        pos_trx_type_cd,
        three_tier_disti_ord_trxtyp_cd,
        src_rptd_shp_cust_cr_gu_id_int,
        src_rptd_bil_cust_cr_gu_id_int,
        src_rptd_sld_cust_cr_gu_id_int,
        src_rptd_end_cust_cr_gu_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_customer_party_key,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM source_w_rbk_cma_enrch_pos_trx_ln
)

SELECT * FROM final