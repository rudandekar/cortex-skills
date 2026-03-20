{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_df_sr_trng_ft_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DF_SR_TRNG_FT_TSS',
        'target_table': 'WI_RSTD_SRC_TRNG_IN_TSS_FT4',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.389393+00:00'
    }
) }}

WITH 

source_wi_rstd_ccw_svc_off_amt_tss_ft1 AS (
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
    FROM {{ source('raw', 'wi_rstd_ccw_svc_off_amt_tss_ft1') }}
),

source_wi_rstd_src_trng_in_tss_ft4 AS (
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
    FROM {{ source('raw', 'wi_rstd_src_trng_in_tss_ft4') }}
),

source_wi_rstd_src_inv_thd_typ_tss_ft7 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_rstd_src_inv_thd_typ_tss_ft7') }}
),

source_wi_rstd_ccw_src_off_inv_tss_ft6 AS (
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
    FROM {{ source('raw', 'wi_rstd_ccw_src_off_inv_tss_ft6') }}
),

source_wi_rstd_src_trng_out_tss_ft5 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_rstd_src_trng_out_tss_ft5') }}
),

source_wi_rstd_src_inv_trn_in_tss_ft8 AS (
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
    FROM {{ source('raw', 'wi_rstd_src_inv_trn_in_tss_ft8') }}
),

source_wi_rstd_fin_svc_thrsd_typ_ft3 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_rstd_fin_svc_thrsd_typ_ft3') }}
),

source_wi_rstd_dfr_ccw_svc_trn_tss_ft2 AS (
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
    FROM {{ source('raw', 'wi_rstd_dfr_ccw_svc_trn_tss_ft2') }}
),

source_wi_rstd_src_inv_trn_out_tss_ft9 AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_rstd_src_inv_trn_out_tss_ft9') }}
),

final AS (
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
    FROM source_wi_rstd_src_inv_trn_out_tss_ft9
)

SELECT * FROM final