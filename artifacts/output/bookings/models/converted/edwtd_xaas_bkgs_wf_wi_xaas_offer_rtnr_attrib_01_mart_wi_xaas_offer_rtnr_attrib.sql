{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_xaas_offer_rtnr_attrib', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_WI_XAAS_OFFER_RTNR_ATTRIB',
        'target_table': 'WI_XAAS_OFFER_RTNR_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.403564+00:00'
    }
) }}

WITH 

source_wi_xaas_offer_rtnr_attrib AS (
    SELECT
        rtnr_unique_id,
        product_key,
        enterprise_sku_prdt_key,
        so_sbscrptn_itm_sls_trx_key,
        attr_prdt_as_new_or_rnwl_key,
        attribution_ss_cd,
        bk_offer_attribution_id_int,
        dv_attribution_cd,
        attribution_pct,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        transaction_bkg_version_int,
        transaction_type_name,
        dv_topsku_sosbr_itm_slstrx_key,
        dv_allocation_pct,
        dv_top_sku_product_key,
        start_tv_dtm
    FROM {{ source('raw', 'wi_xaas_offer_rtnr_attrib') }}
),

final AS (
    SELECT
        rtnr_unique_id,
        product_key,
        enterprise_sku_prdt_key,
        so_sbscrptn_itm_sls_trx_key,
        attr_prdt_as_new_or_rnwl_key,
        attribution_ss_cd,
        bk_offer_attribution_id_int,
        dv_attribution_cd,
        attribution_pct,
        sales_motion_cd,
        sk_rtnr_attribution_id_int,
        sk_trx_id_int,
        transaction_bkg_version_int,
        transaction_type_name,
        dv_topsku_sosbr_itm_slstrx_key,
        dv_allocation_pct,
        dv_top_sku_product_key,
        start_tv_dtm
    FROM source_wi_xaas_offer_rtnr_attrib
)

SELECT * FROM final