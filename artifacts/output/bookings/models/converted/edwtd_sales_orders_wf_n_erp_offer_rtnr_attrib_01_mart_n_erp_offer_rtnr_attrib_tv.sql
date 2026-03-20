{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_erp_offer_rtnr_attrib', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_ERP_OFFER_RTNR_ATTRIB',
        'target_table': 'N_ERP_OFFER_RTNR_ATTRIB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.052272+00:00'
    }
) }}

WITH 

source_n_erp_offer_rtnr_attrib_tv AS (
    SELECT
        offer_attrib_acv_rtnr_erp_key,
        bk_offer_attribution_id_int,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_erp_offer_rtnr_attrib_tv') }}
),

source_n_erp_offer_rtnr_attrib AS (
    SELECT
        offer_attrib_acv_rtnr_erp_key,
        bk_offer_attribution_id_int,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_erp_offer_rtnr_attrib') }}
),

final AS (
    SELECT
        offer_attrib_acv_rtnr_erp_key,
        bk_offer_attribution_id_int,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        rtnr_unique_id,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        sk_sales_motion_attrib_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_erp_offer_rtnr_attrib
)

SELECT * FROM final