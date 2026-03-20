{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_df_ccw_srvc_brdg_fix_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DF_CCW_SRVC_BRDG_FIX_TSS',
        'target_table': 'WI_RSTD_DR_CCW_SVC_BRDG_TRNG_TSS_28',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.281206+00:00'
    }
) }}

WITH 

source_wi_rstd_dr_ccw_svc_brdg_trng_tss_28 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_svc_brdg_trng_tss_28') }}
),

source_wi_rstd_dr_ccw_src_brdg_ft_tss_26 AS (
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
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_src_brdg_ft_tss_26') }}
),

source_wi_rstd_dr_ccw_src_brdg_off_tss_27 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_src_brdg_off_tss_27') }}
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
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_wi_rstd_dr_ccw_src_brdg_off_tss_27
)

SELECT * FROM final