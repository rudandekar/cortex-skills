{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_qtr_not_mapped_products', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_QTR_NOT_MAPPED_PRODUCTS',
        'target_table': 'WI_QTR_NOT_MAPPED_PRODUCTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.933544+00:00'
    }
) }}

WITH 

source_r_products_fin AS (
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
        service_flg
    FROM {{ source('raw', 'r_products_fin') }}
),

final AS (
    SELECT
        not_mapped_item_key
    FROM source_r_products_fin
)

SELECT * FROM final