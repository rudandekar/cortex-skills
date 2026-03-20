{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rbk_cma_enrch_xaas_slstrx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_RBK_CMA_ENRCH_XAAS_SLSTRX',
        'target_table': 'EX_CMA_TRX_PRTS_EXT_MNL_TRX_XA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.974431+00:00'
    }
) }}

WITH 

source_wi_rbk_cma_enrch_xaas_slstrx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        ship_to_cust_party_key,
        bill_to_cust_party_key,
        sold_to_cust_party_key,
        end_user_theater_name,
        ship_to_theater_name,
        src_rptd_shp_cust_cr_gu_id_int,
        src_rptd_bil_cust_cr_gu_id_int,
        src_rptd_sld_cust_cr_gu_id_int,
        src_rptd_end_cust_cr_gu_id_int,
        end_customer_party_key,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'wi_rbk_cma_enrch_xaas_slstrx') }}
),

source_w_rbk_cma_enrch_xaas_slstrx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        ship_to_cust_party_key,
        bill_to_cust_party_key,
        sold_to_cust_party_key,
        end_user_theater_name,
        ship_to_theater_name,
        src_rptd_shp_cust_cr_gu_id_int,
        src_rptd_bil_cust_cr_gu_id_int,
        src_rptd_sld_cust_cr_gu_id_int,
        src_rptd_end_cust_cr_gu_id_int,
        end_customer_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rbk_cma_enrch_xaas_slstrx') }}
),

source_ex_cma_trx_prts_ext_mnl_trx_xa AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        trx_orig_code,
        trx_source_type,
        last_update_date,
        batch_id,
        exception_type
    FROM {{ source('raw', 'ex_cma_trx_prts_ext_mnl_trx_xa') }}
),

source_st_cma_trx_prts_ext_mnl_trx AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        trx_orig_code,
        trx_source_type,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_cma_trx_prts_ext_mnl_trx') }}
),

final AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        trx_orig_code,
        trx_source_type,
        last_update_date,
        batch_id,
        exception_type
    FROM source_st_cma_trx_prts_ext_mnl_trx
)

SELECT * FROM final