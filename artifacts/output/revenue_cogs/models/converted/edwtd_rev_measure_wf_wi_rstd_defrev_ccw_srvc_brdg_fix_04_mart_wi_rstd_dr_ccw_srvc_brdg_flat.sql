{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_defrev_ccw_srvc_brdg_fix', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DEFREV_CCW_SRVC_BRDG_FIX',
        'target_table': 'WI_RSTD_DR_CCW_SRVC_BRDG_FLAT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.713475+00:00'
    }
) }}

WITH 

source_wi_rstd_srvc_brdg_trngt_out AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_rstd_srvc_brdg_trngt_out') }}
),

source_wi_rstd_srvc_brdg_thresh_type AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_rstd_srvc_brdg_thresh_type') }}
),

source_wi_rstd_dr_ccw_srvc_brdg_flat AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_srvc_brdg_flat') }}
),

source_wi_rstd_srvc_brdg_trngt_in AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        l3_sales_territory_name_code,
        iso_country_code,
        product_key,
        bk_product_id,
        ru_bk_product_family_id,
        bk_business_entity_name,
        prdt_family_allocation_pct
    FROM {{ source('raw', 'wi_rstd_srvc_brdg_trngt_in') }}
),

source_wi_rstd_dr_ccw_srvc_brdg_offer AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_srvc_brdg_offer') }}
),

source_wi_rstd_dr_ccw_svc_brdg_trngld AS (
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
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_dr_ccw_svc_brdg_trngld') }}
),

final AS (
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
        restated_sls_crdt_split_pct
    FROM source_wi_rstd_dr_ccw_svc_brdg_trngld
)

SELECT * FROM final