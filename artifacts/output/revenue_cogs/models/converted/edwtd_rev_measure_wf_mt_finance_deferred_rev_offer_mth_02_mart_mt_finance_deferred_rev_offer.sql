{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_finance_deferred_rev_offer_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_FINANCE_DEFERRED_REV_OFFER_MTH',
        'target_table': 'MT_FINANCE_DEFERRED_REV_OFFER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.518395+00:00'
    }
) }}

WITH 

source_mt_finance_deferred_rev_offer AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_period_number,
        fiscal_period_type,
        bk_measure_name,
        product_key,
        src_entity_name,
        l2_offer_type_name,
        l1_offer_type_name,
        dv_offer_l2_cust_name,
        dv_offer_component_name,
        dv_offer_group_name,
        dv_offer_category_name,
        dv_offer_cons_category_name,
        dv_offer_ela_flg,
        ela_flg,
        xcat_flg,
        product_subscription_flg,
        scenario_name,
        dv_beginning_bal_rev_usd_amt,
        deferral_rev_usd_amt,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_product_id,
        bk_product_family_id,
        bk_business_entity_name,
        bk_sub_business_entity_name,
        level05_theater_name,
        l3_sales_territory_name_code,
        l3_sales_territory_descr,
        place_in_network_name
    FROM {{ source('raw', 'mt_finance_deferred_rev_offer') }}
),

source_wi_defrev_offrvw_monthly_flat AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_period_number,
        fiscal_period_type,
        bk_measure_name,
        product_key,
        src_entity_name,
        l2_offer_type_name,
        l1_offer_type_name,
        dv_offer_l2_cust_name,
        dv_offer_component_name,
        dv_offer_group_name,
        dv_offer_category_name,
        dv_offer_cons_category_name,
        dv_offer_ela_flg,
        ela_flg,
        xcat_flg,
        product_subscription_flg,
        scenario_name,
        dv_beginning_bal_rev_usd_amt,
        deferral_rev_usd_amt,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        bk_product_id,
        bk_product_family_id,
        bk_business_entity_name,
        bk_sub_business_entity_name,
        level05_theater_name,
        l3_sales_territory_name_code,
        l3_sales_territory_descr
    FROM {{ source('raw', 'wi_defrev_offrvw_monthly_flat') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_period_number,
        fiscal_period_type,
        bk_measure_name,
        product_key,
        src_entity_name,
        l2_offer_type_name,
        l1_offer_type_name,
        dv_offer_l2_cust_name,
        dv_offer_component_name,
        dv_offer_group_name,
        dv_offer_category_name,
        dv_offer_cons_category_name,
        dv_offer_ela_flg,
        ela_flg,
        xcat_flg,
        product_subscription_flg,
        scenario_name,
        dv_beginning_bal_rev_usd_amt,
        deferral_rev_usd_amt,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_product_id,
        bk_product_family_id,
        bk_business_entity_name,
        bk_sub_business_entity_name,
        level05_theater_name,
        l3_sales_territory_name_code,
        l3_sales_territory_descr,
        place_in_network_name
    FROM source_wi_defrev_offrvw_monthly_flat
)

SELECT * FROM final