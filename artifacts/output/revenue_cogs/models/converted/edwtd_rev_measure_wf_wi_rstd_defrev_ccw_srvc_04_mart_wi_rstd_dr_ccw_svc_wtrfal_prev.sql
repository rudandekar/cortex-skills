{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_defrev_ccw_srvc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DEFREV_CCW_SRVC',
        'target_table': 'WI_RSTD_DR_CCW_SVC_WTRFAL_PREV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.707169+00:00'
    }
) }}

WITH 

source_wi_rstd_defrev_ccw_srvc AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_defrev_ccw_srvc') }}
),

source_wi_rstd_dr_ccw_discont_mth_svc AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_discont_mth_svc') }}
),

source_wi_rstd_dr_ccw_svc_wtrfal_prev AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_svc_wtrfal_prev') }}
),

source_wi_rstd_defrev_ccw_srvc_wtrfal AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_defrev_ccw_srvc_wtrfal') }}
),

source_wi_rstd_dr_ccw_srvc_ar_trx_key AS (
    SELECT
        ar_trx_key
    FROM {{ source('raw', 'wi_rstd_dr_ccw_srvc_ar_trx_key') }}
),

source_wi_rstd_dr_ccw_max_mth_srvc AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_max_mth_srvc') }}
),

final AS (
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
        restated_sls_crdt_split_pct
    FROM source_wi_rstd_dr_ccw_max_mth_srvc
)

SELECT * FROM final