{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_erp_offer_rtnr_attrib', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_ERP_OFFER_RTNR_ATTRIB',
        'target_table': 'WI_ERP_OFFER_RTNR_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.339056+00:00'
    }
) }}

WITH 

source_ex_erp_offer_rtnr_attrib AS (
    SELECT
        rtnr_unique_id,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        bk_offer_attribution_id_int,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        start_tv_dtm,
        exception_type,
        exception_reason
    FROM {{ source('raw', 'ex_erp_offer_rtnr_attrib') }}
),

source_wi_erp_offer_rtnr_attrib AS (
    SELECT
        rtnr_unique_id,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        bk_offer_attribution_id_int,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        start_tv_dtm
    FROM {{ source('raw', 'wi_erp_offer_rtnr_attrib') }}
),

final AS (
    SELECT
        rtnr_unique_id,
        attribution_ss_cd,
        dv_topsku_sales_order_line_key,
        attr_prdt_as_new_or_rnwl_key,
        dv_attribution_cd,
        attribution_pct,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        sales_order_line_key,
        bk_offer_attribution_id_int,
        transaction_type_name,
        product_key,
        dv_top_sku_product_key,
        enterprise_sku_prdt_key,
        bookings_policy_cd,
        dv_allocation_pct,
        start_tv_dtm
    FROM source_wi_erp_offer_rtnr_attrib
)

SELECT * FROM final