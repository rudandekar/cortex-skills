{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_offrvw_mth_yr_vetr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_OFFRVW_MTH_YR_VETR',
        'target_table': 'WI_DEFREV_OFFRVW_MONTHLY_VETR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.796143+00:00'
    }
) }}

WITH 

source_wi_defrev_offrvw_monthly_vetr AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_period_number,
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
        rev_measurement_type_cd,
        defrev_amt,
        bk_product_id,
        bk_product_family_id,
        bk_business_entity_name,
        bk_sub_business_entity_name,
        level05_theater_name,
        l3_sales_territory_name_code,
        l3_sales_territory_descr
    FROM {{ source('raw', 'wi_defrev_offrvw_monthly_vetr') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_period_number,
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
        rev_measurement_type_cd,
        defrev_amt,
        bk_product_id,
        bk_product_family_id,
        bk_business_entity_name,
        bk_sub_business_entity_name,
        level05_theater_name,
        l3_sales_territory_name_code,
        l3_sales_territory_descr
    FROM source_wi_defrev_offrvw_monthly_vetr
)

SELECT * FROM final