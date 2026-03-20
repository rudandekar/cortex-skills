{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_fin_dfr_ccw_srvc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_FIN_DFR_CCW_SRVC',
        'target_table': 'WI_RSTD_FIN_DFR_CCW_SRVC4',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.900219+00:00'
    }
) }}

WITH 

source_wi_rstd_fin_ccw_max_mth_sr5 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_deal_id,
        fiscal_year_month_int,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_ccw_max_mth_sr5') }}
),

source_wi_rstd_fin_dfr_ccw_srvc1 AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        product_key,
        sales_territory_key,
        bk_deal_id,
        comp_us_net_rev_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        ar_trx_key,
        trans_type_category_cd,
        revenue_transfer_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_dfr_ccw_srvc1') }}
),

source_wi_rstd_fin_ccw_svc_prev2 AS (
    SELECT
        processed_fiscal_year_mth_int,
        product_key,
        sales_territory_key,
        bk_deal_id,
        comp_us_net_rev_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        ar_trx_key,
        revenue_transfer_key,
        trans_type_category_cd,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_ccw_svc_prev2') }}
),

source_wi_rstd_fin_dfr_ccw_srvc4 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        rev_measurement_type_cd,
        deferred_rev_usd_amt,
        ar_trx_key,
        dv_corporate_revenue_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_dfr_ccw_srvc4') }}
),

source_wi_rstd_fin_ccw_srvc_ar_trx3 AS (
    SELECT
        ar_trx_key
    FROM {{ source('raw', 'wi_rstd_fin_ccw_srvc_ar_trx3') }}
),

source_wi_rstd_fin_ccw_mth_svc6 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_ccw_mth_svc6') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        rev_measurement_type_cd,
        deferred_rev_usd_amt,
        ar_trx_key,
        dv_corporate_revenue_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        net_price_flg
    FROM source_wi_rstd_fin_ccw_mth_svc6
)

SELECT * FROM final