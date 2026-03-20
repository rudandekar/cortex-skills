{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ssc_id_product_key', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SSC_ID_PRODUCT_KEY',
        'target_table': 'WI_CCM_SSC_ID_PRODUCT_KEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.790996+00:00'
    }
) }}

WITH 

source_wi_ccm_ssc_gp_sbe_split_ratio AS (
    SELECT
        sub_business_entity_name,
        goods_product_key,
        product_key_ratio
    FROM {{ source('raw', 'wi_ccm_ssc_gp_sbe_split_ratio') }}
),

source_n_sr_srvc_support_center_cost AS (
    SELECT
        service_request_ssc_cost_key,
        src_rptd_sr_number_lint,
        src_rptd_service_contarct_num,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        sr_product_subgroup_id,
        sr_allocated_service_group_id,
        sr_service_category_id,
        sr_product_family_id,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        sk_ccm_ssc_id_int,
        fiscal_year_month_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sr_srvc_support_center_cost') }}
),

source_wi_ccm_ts_mth_qtr_cntl AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        ccm_fiscal_month,
        month_rank
    FROM {{ source('raw', 'wi_ccm_ts_mth_qtr_cntl') }}
),

source_r_products AS (
    SELECT
        bk_product_id,
        base_product_id,
        goods_or_service_type,
        product_status_code,
        product_description,
        product_active_flag,
        cisco_finished_prod_type_code,
        unannounced_product_flag,
        adjustment_product_flag,
        ru_goods_product_type,
        ru_udi_compliance_flag,
        ru_rohs_compliance_flag,
        ru_os_type_code,
        ru_os_featureset_name,
        ru_os_featureset_description,
        ru_os_release_number,
        ru_os_image_name,
        ru_os_image_description,
        ru_commissionable_status_code,
        ru_customer_orderable_flag,
        bk_product_type_id,
        bk_item_type_code,
        bk_item_class_code,
        ru_bk_product_family_id,
        ru_bk_product_subgroup_id,
        ru_bk_service_prod_subgroup_id,
        product_accounting_rule,
        bk_business_unit_id,
        product_family_description,
        product_family_active_flag,
        service_subgroup_type,
        product_subgroup_active_flag,
        ru_bk_allocated_servc_group_id,
        ru_service_type,
        ru_generic_servc_product_role,
        ru_service_brand_code,
        ru_service_level_group_code,
        ru_gsp_status_code,
        ru_gsp_type,
        ru_bk_service_program_id,
        product_subgroup_description,
        product_type_active_flag,
        bk_manufacturing_plant_id,
        bk_technology_group_id,
        business_unit_description,
        business_unit_active_flag,
        technology_group_code,
        technology_group_description,
        technology_group_active_flag,
        item_key,
        ru_os_software_role,
        product_family_trgt_lead_time,
        shippable_flg,
        bk_product_hier_company_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_software_subscription_cd,
        new_product_flg,
        product_pricing_category_cd,
        first_customer_ship_dt,
        serviceable_product_flg,
        dv_item_type_cd,
        service_flg,
        product_planning_life_cycle_cd,
        program_name,
        prod_planning_classf_cd,
        product_bundle_flg,
        prod_planning_attach_rate_flg,
        bk_planning_id
    FROM {{ source('raw', 'r_products') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        service_request_ssc_cost_key,
        goods_product_key,
        product_key_ratio
    FROM source_r_products
)

SELECT * FROM final