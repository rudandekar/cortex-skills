{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_xaas_offer_rtnr_attrib', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_XAAS_OFFER_RTNR_ATTRIB',
        'target_table': 'N_XAAS_OFFER_RTNR_ATTRIB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.938292+00:00'
    }
) }}

WITH 

source_n_xaas_offer_rtnr_attrib_tv AS (
    SELECT
        offer_attrib_acv_rtnr_xaas_key,
        product_key,
        enterprise_sku_prdt_key,
        so_sbscrptn_itm_sls_trx_key,
        attr_prdt_as_new_or_rnwl_key,
        attribution_ss_cd,
        bk_offer_attribution_id_int,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        transaction_bkg_version_int,
        transaction_type_name,
        dv_topsku_sosbr_itm_slstrx_key,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        dv_top_sku_product_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_xaas_offer_rtnr_attrib_tv') }}
),

source_w_xaas_offer_rtnr_attrib AS (
    SELECT
        offer_attrib_acv_rtnr_xaas_key,
        product_key,
        enterprise_sku_prdt_key,
        so_sbscrptn_itm_sls_trx_key,
        attr_prdt_as_new_or_rnwl_key,
        attribution_ss_cd,
        bk_offer_attribution_id_int,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        transaction_bkg_version_int,
        transaction_type_name,
        dv_topsku_sosbr_itm_slstrx_key,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        dv_top_sku_product_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_xaas_offer_rtnr_attrib') }}
),

final AS (
    SELECT
        offer_attrib_acv_rtnr_xaas_key,
        product_key,
        enterprise_sku_prdt_key,
        so_sbscrptn_itm_sls_trx_key,
        attr_prdt_as_new_or_rnwl_key,
        attribution_ss_cd,
        bk_offer_attribution_id_int,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        transaction_bkg_version_int,
        transaction_type_name,
        dv_topsku_sosbr_itm_slstrx_key,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        dv_top_sku_product_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_xaas_offer_rtnr_attrib
)

SELECT * FROM final