{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_dfr_ccw_srvc_net_trng_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DFR_CCW_SRVC_NET_TRNG_TSS',
        'target_table': 'WI_RSTD_DFR_CCW_SRC_NET_TSS_19',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.861061+00:00'
    }
) }}

WITH 

source_wi_rstd_dfr_ccw_src_net_tss_19 AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dfr_ccw_src_net_tss_19') }}
),

source_wi_rstd_dr_ccw_src_net_ft_tss_20 AS (
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
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_src_net_ft_tss_20') }}
),

source_wi_rstd_dr_ccw_svc_net_trngt_tss_22 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_svc_net_trngt_tss_22') }}
),

source_wi_rstd_dr_ccw_src_net_off_tss_21 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_src_net_off_tss_21') }}
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
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_wi_rstd_dr_ccw_src_net_off_tss_21
)

SELECT * FROM final