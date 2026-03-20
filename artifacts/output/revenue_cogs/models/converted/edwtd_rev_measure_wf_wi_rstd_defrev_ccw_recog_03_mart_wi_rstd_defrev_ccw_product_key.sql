{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_defrev_ccw_recog', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DEFREV_CCW_RECOG',
        'target_table': 'WI_RSTD_DEFREV_CCW_PRODUCT_KEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.990020+00:00'
    }
) }}

WITH 

source_wi_rstd_defrev_vip_auto_recog AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        product_key,
        orig_sales_territory_key,
        sales_territory_key,
        bk_deal_id,
        dv_shipped_revenue_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd
    FROM {{ source('raw', 'wi_rstd_defrev_vip_auto_recog') }}
),

source_wi_rstd_defrev_ccw_product_key AS (
    SELECT
        product_key,
        dv_attribution_cd,
        dv_product_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_rstd_defrev_ccw_product_key') }}
),

source_wi_rstd_defrev_ccw_recog AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        product_key,
        orig_sales_territory_key,
        sales_territory_key,
        bk_deal_id,
        dv_shipped_revenue_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd
    FROM {{ source('raw', 'wi_rstd_defrev_ccw_recog') }}
),

final AS (
    SELECT
        product_key,
        dv_attribution_cd,
        dv_product_key,
        dv_ar_trx_line_key
    FROM source_wi_rstd_defrev_ccw_recog
)

SELECT * FROM final