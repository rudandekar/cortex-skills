{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_df_sr_brdg_ft_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DF_SR_BRDG_FT_TSS',
        'target_table': 'WI_RSTD_DR_CCW_SR_BRD_TRN_TSS_FT15',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.909362+00:00'
    }
) }}

WITH 

source_wi_rstd_sr_brd_tn_out_tss_ft19 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_rstd_sr_brd_tn_out_tss_ft19') }}
),

source_wi_rstd_dr_brd_thr_typ_tn_tss_ft17 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_rstd_dr_brd_thr_typ_tn_tss_ft17') }}
),

source_wi_rstd_sr_brd_tn_in_tss_ft18 AS (
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
    FROM {{ source('raw', 'wi_rstd_sr_brd_tn_in_tss_ft18') }}
),

source_wi_rstd_dr_ccw_sr_brd_trn_tss_ft15 AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_rstd_dr_ccw_sr_brd_trn_tss_ft15') }}
),

source_wi_rstd_dr_ccw_sr_brd_trn_tss_ft16 AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_rstd_dr_ccw_sr_brd_trn_tss_ft16') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM source_wi_rstd_dr_ccw_sr_brd_trn_tss_ft16
)

SELECT * FROM final