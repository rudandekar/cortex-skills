{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_dfr_ccw_srvc_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DFR_CCW_SRVC_TSS',
        'target_table': 'WI_RSTD_DFR_CCW_SRVC_WF_TSS_1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.134976+00:00'
    }
) }}

WITH 

source_wi_rstd_ccw_max_mth_sr_tss_5 AS (
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
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_ccw_max_mth_sr_tss_5') }}
),

source_wi_rstd_dfr_ccw_srvc_wf_tss_1 AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_dfr_ccw_srvc_wf_tss_1') }}
),

source_wi_rstd_ccw_svc_wf_prev_tss_2 AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_ccw_svc_wf_prev_tss_2') }}
),

source_wi_rstd_dfr_ccw_srvc_tss_4 AS (
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
    FROM {{ source('raw', 'wi_rstd_dfr_ccw_srvc_tss_4') }}
),

source_wi_rstd_ccw_srvc_ar_trx_tss_3 AS (
    SELECT
        ar_trx_key
    FROM {{ source('raw', 'wi_rstd_ccw_srvc_ar_trx_tss_3') }}
),

source_wi_rstd_ccw_dis_mth_svc_tss_6 AS (
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
        restated_sls_crdt_split_pct,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_rstd_ccw_dis_mth_svc_tss_6') }}
),

final AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_wi_rstd_ccw_dis_mth_svc_tss_6
)

SELECT * FROM final