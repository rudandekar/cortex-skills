{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ar_offer_rtnr_attrib', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_AR_OFFER_RTNR_ATTRIB',
        'target_table': 'EX_WI_AR_OFFER_RTNR_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.339962+00:00'
    }
) }}

WITH 

source_ex_wi_ar_offer_rtnr_attrib AS (
    SELECT
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        revenue_transfer_key,
        bk_offer_attribution_id_int,
        dv_transaction_source_cd,
        dv_sales_order_line_key,
        sbscrptn_item_change_id_int,
        product_key,
        dv_product_key,
        attribution_pct,
        dv_attribution_cd,
        bk_batch_source_name,
        trx_source_cd,
        so_sbscrptn_itm_sls_trx_key,
        lnkd_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        edw_create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_wi_ar_offer_rtnr_attrib') }}
),

source_wi_ar_offer_rtnr_attrib AS (
    SELECT
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        revenue_transfer_key,
        bk_offer_attribution_id_int,
        dv_transaction_source_cd,
        dv_sales_order_line_key,
        sbscrptn_item_change_id_int,
        product_key,
        dv_product_key,
        attribution_pct,
        dv_attribution_cd,
        bk_batch_source_name,
        trx_source_cd,
        so_sbscrptn_itm_sls_trx_key,
        lnkd_offer_attribution_id_int,
        sk_sales_motion_attrib_key
    FROM {{ source('raw', 'wi_ar_offer_rtnr_attrib') }}
),

source_wi_ar_offer_rtnr_attrib_fnl AS (
    SELECT
        batch_id,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        revenue_transfer_key,
        bk_offer_attribution_id_int,
        dv_transaction_source_cd,
        dv_sales_order_line_key,
        sbscrptn_item_change_id_int,
        product_key,
        dv_product_key,
        attribution_pct,
        dv_attribution_cd,
        bk_batch_source_name,
        trx_source_cd,
        so_sbscrptn_itm_sls_trx_key,
        lnkd_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        rtnr_unique_id,
        sk_rtnr_attribution_id_int,
        sales_motion_cd,
        dv_allocation_pct
    FROM {{ source('raw', 'wi_ar_offer_rtnr_attrib_fnl') }}
),

source_wi_ar_offer_rtnr_attrib_incr AS (
    SELECT
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        revenue_transfer_key,
        sk_offer_attribution_id_int,
        dv_transaction_source_cd,
        dv_sales_order_line_key,
        sbscrptn_item_change_id_int,
        product_key,
        dv_product_key,
        attribution_pct,
        dv_attribution_cd,
        bk_batch_source_name,
        trx_source_cd
    FROM {{ source('raw', 'wi_ar_offer_rtnr_attrib_incr') }}
),

final AS (
    SELECT
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        revenue_transfer_key,
        bk_offer_attribution_id_int,
        dv_transaction_source_cd,
        dv_sales_order_line_key,
        sbscrptn_item_change_id_int,
        product_key,
        dv_product_key,
        attribution_pct,
        dv_attribution_cd,
        bk_batch_source_name,
        trx_source_cd,
        so_sbscrptn_itm_sls_trx_key,
        lnkd_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        edw_create_datetime,
        exception_type
    FROM source_wi_ar_offer_rtnr_attrib_incr
)

SELECT * FROM final