{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rbk_cma_enrch_pos_trx_ln_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_RBK_CMA_ENRCH_POS_TRX_LN_TV',
        'target_table': 'N_RBK_CMA_ENRCH_POS_TRX_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.900459+00:00'
    }
) }}

WITH 

source_n_rbk_cma_enrch_pos_trx_ln_tv AS (
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
        end_tv_dt
    FROM {{ source('raw', 'n_rbk_cma_enrch_pos_trx_ln_tv') }}
),

source_n_rbk_cma_enrch_pos_trx_ln AS (
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_rbk_cma_enrch_pos_trx_ln') }}
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
        end_customer_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_rbk_cma_enrch_pos_trx_ln
)

SELECT * FROM final