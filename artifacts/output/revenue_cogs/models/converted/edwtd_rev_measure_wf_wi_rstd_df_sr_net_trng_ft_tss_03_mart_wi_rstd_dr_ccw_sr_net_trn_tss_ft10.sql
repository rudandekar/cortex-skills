{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_df_sr_net_trng_ft_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DF_SR_NET_TRNG_FT_TSS',
        'target_table': 'WI_RSTD_DR_CCW_SR_NET_TRN_TSS_FT10',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.151542+00:00'
    }
) }}

WITH 

source_wi_rstd_src_net_trng_out_tss_ft14 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_rstd_src_net_trng_out_tss_ft14') }}
),

source_wi_rstd_svc_net_thsh_type_tss_ft12 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_rstd_svc_net_thsh_type_tss_ft12') }}
),

source_wi_rstd_src_net_trng_in_tss_ft13 AS (
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
    FROM {{ source('raw', 'wi_rstd_src_net_trng_in_tss_ft13') }}
),

source_wi_rstd_dr_ccw_sr_net_trn_tss_ft10 AS (
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
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        deferral_rev_usd_amt,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_rstd_dr_ccw_sr_net_trn_tss_ft10') }}
),

source_wi_rstd_dr_ccw_sr_net_trn_tss_ft11 AS (
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
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        deferral_rev_usd_amt,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_rstd_dr_ccw_sr_net_trn_tss_ft11') }}
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
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        deferral_rev_usd_amt,
        dv_goods_product_key,
        rev_measurement_type_cd,
        record_type,
        dv_recurring_offer_cd
    FROM source_wi_rstd_dr_ccw_sr_net_trn_tss_ft11
)

SELECT * FROM final