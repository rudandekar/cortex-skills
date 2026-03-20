{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_unbilled_revenue_non_rec_rev', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_MT_UNBILLED_REVENUE_NON_REC_REV',
        'target_table': 'MT_UNBILLED_REVENUE_NON_REC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.486707+00:00'
    }
) }}

WITH 

source_mt_unbilled_revenue_non_rec AS (
    SELECT
        transaction_type,
        newfield,
        unbilled_revenue_amt,
        processed_fiscal_mth,
        fiscal_mth,
        src_entity,
        sales_territory_key,
        sales_order_line_key,
        service_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        attributed_product_key,
        offer_type_cd,
        ato_name,
        dv_recurring_offer_cd,
        dv_prdt_allctn_clsfctn_cd,
        dv_cx_product,
        dv_ato_product_key
    FROM {{ source('raw', 'mt_unbilled_revenue_non_rec') }}
),

final AS (
    SELECT
        transaction_type,
        unbilled_revenue_amt,
        processed_fiscal_mth,
        fiscal_mth,
        src_entity,
        sales_territory_key,
        sales_order_line_key,
        service_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        attributed_product_key,
        offer_type_cd,
        ato_name,
        dv_recurring_offer_cd,
        dv_prdt_allctn_clsfctn_cd,
        dv_cx_product,
        dv_ato_product_key
    FROM source_mt_unbilled_revenue_non_rec
)

SELECT * FROM final