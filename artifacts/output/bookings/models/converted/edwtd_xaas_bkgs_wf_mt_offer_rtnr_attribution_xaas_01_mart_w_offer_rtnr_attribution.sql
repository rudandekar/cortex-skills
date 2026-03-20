{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_offer_rtnr_attribution_xaas', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_MT_OFFER_RTNR_ATTRIBUTION_XAAS',
        'target_table': 'W_OFFER_RTNR_ATTRIBUTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.480812+00:00'
    }
) }}

WITH 

source_w_offer_rtnr_attribution AS (
    SELECT
        offer_attr_acv_rtnr_key,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        rtnr_unique_id,
        sk_trx_id_int,
        bk_offer_attribution_id_int,
        sales_motion_cd,
        attr_prdt_as_new_or_rnwl_key,
        attribution_pct,
        product_key,
        dv_product_key,
        dv_sales_order_line_key,
        sales_order_line_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        so_sbscrptn_itm_sls_trx_key,
        transaction_bkg_version_int,
        dv_ar_trx_line_key,
        revenue_transfer_key,
        xaas_offer_atrbtn_rev_line_key,
        transaction_type_name,
        sk_sales_motion_attrib_key,
        dv_allocation_pct,
        enterprise_sku_prdt_key,
        lnkd_offer_attribution_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_offer_rtnr_attribution') }}
),

source_mt_offer_rtnr_attribution AS (
    SELECT
        offer_attr_acv_rtnr_key,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        rtnr_unique_id,
        sk_trx_id_int,
        bk_offer_attribution_id_int,
        sales_motion_cd,
        attr_prdt_as_new_or_rnwl_key,
        attribution_pct,
        product_key,
        dv_product_key,
        dv_sales_order_line_key,
        sales_order_line_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        so_sbscrptn_itm_sls_trx_key,
        transaction_bkg_version_int,
        dv_ar_trx_line_key,
        revenue_transfer_key,
        xaas_offer_atrbtn_rev_line_key,
        transaction_type_name,
        sk_sales_motion_attrib_key,
        dv_allocation_pct,
        enterprise_sku_prdt_key,
        lnkd_offer_attribution_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_offer_rtnr_attribution') }}
),

final AS (
    SELECT
        offer_attr_acv_rtnr_key,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        rtnr_unique_id,
        sk_trx_id_int,
        bk_offer_attribution_id_int,
        sales_motion_cd,
        attr_prdt_as_new_or_rnwl_key,
        attribution_pct,
        product_key,
        dv_product_key,
        dv_sales_order_line_key,
        sales_order_line_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        so_sbscrptn_itm_sls_trx_key,
        transaction_bkg_version_int,
        dv_ar_trx_line_key,
        revenue_transfer_key,
        xaas_offer_atrbtn_rev_line_key,
        transaction_type_name,
        sk_sales_motion_attrib_key,
        dv_allocation_pct,
        enterprise_sku_prdt_key,
        lnkd_offer_attribution_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_mt_offer_rtnr_attribution
)

SELECT * FROM final