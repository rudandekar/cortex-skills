{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_fin_dfr_ccw_srvc_net_trng', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_FIN_DFR_CCW_SRVC_NET_TRNG',
        'target_table': 'WI_RSTD_FIN_DR_CCW_NET_TRG22',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.331839+00:00'
    }
) }}

WITH 

source_wi_rstd_fin_ccw_src_net_ft20 AS (
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
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_ccw_src_net_ft20') }}
),

source_wi_rstd_fin_ccw_src_net_off21 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_ccw_src_net_off21') }}
),

source_wi_rstd_fin_dfr_ccw_src_net19 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        rev_measurement_type_cd,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_dfr_ccw_src_net19') }}
),

source_wi_rstd_fin_dr_ccw_net_trg22 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'wi_rstd_fin_dr_ccw_net_trg22') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_key,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM source_wi_rstd_fin_dr_ccw_net_trg22
)

SELECT * FROM final