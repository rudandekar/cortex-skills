{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_offer_dfrd_rev_frcst', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_FIN_OFFER_DFRD_REV_FRCST',
        'target_table': 'N_FIN_OFFER_DFRD_REV_FRCST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.127347+00:00'
    }
) }}

WITH 

source_n_fin_offer_dfrd_rev_frcst AS (
    SELECT
        fin_offer_dfrd_rev_frcst_key,
        bk_dfrd_fiscal_period_num_int,
        bk_fiscal_period_type_cd,
        bk_offer_component_name,
        bk_offer_group_name,
        bk_offer_category_name,
        bk_measure_name,
        bk_src_entity_name,
        bk_src_rptd_offr_lvl_cust_name,
        bk_ela_flg,
        bk_cross_catalog_flg,
        product_key,
        bk_prcssd_fscl_cal_cd,
        bk_prcssd_fscl_yr_num_int,
        bk_prcssd_fscl_mth_num_int,
        bk_level_2_offer_type_name,
        bk_level_1_offer_type_name,
        offr_cnsldtd_ctgry_name,
        offer_ela_flg,
        src_rptd_prdt_subscr_flg,
        deferrment_scenario_name,
        beginning_dfrd_rev_usd_amt,
        deferred_rev_usd_amt,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        bk_business_entity_type_cd,
        bk_sub_business_entity_name,
        bk_business_entity_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        level_5_theater_name,
        level_3_sales_terr_name,
        level_3_sales_terr_descr
    FROM {{ source('raw', 'n_fin_offer_dfrd_rev_frcst') }}
),

final AS (
    SELECT
        fin_offer_dfrd_rev_frcst_key,
        bk_dfrd_fiscal_period_num_int,
        bk_fiscal_period_type_cd,
        bk_offer_component_name,
        bk_offer_group_name,
        bk_offer_category_name,
        bk_measure_name,
        bk_src_entity_name,
        bk_src_rptd_offr_lvl_cust_name,
        bk_ela_flg,
        bk_cross_catalog_flg,
        product_key,
        bk_prcssd_fscl_cal_cd,
        bk_prcssd_fscl_yr_num_int,
        bk_prcssd_fscl_mth_num_int,
        bk_level_2_offer_type_name,
        bk_level_1_offer_type_name,
        offr_cnsldtd_ctgry_name,
        offer_ela_flg,
        src_rptd_prdt_subscr_flg,
        deferrment_scenario_name,
        beginning_dfrd_rev_usd_amt,
        deferred_rev_usd_amt,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        bk_business_entity_type_cd,
        bk_sub_business_entity_name,
        bk_business_entity_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        level_5_theater_name,
        level_3_sales_terr_name,
        level_3_sales_terr_descr
    FROM source_n_fin_offer_dfrd_rev_frcst
)

SELECT * FROM final