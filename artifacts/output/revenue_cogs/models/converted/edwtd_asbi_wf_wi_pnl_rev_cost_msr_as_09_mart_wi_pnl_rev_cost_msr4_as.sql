{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_AS',
        'target_table': 'WI_PNL_REV_COST_MSR4_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.657047+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_pnl_rev_cost_msr1_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        sales_territory_key,
        product_subgroup,
        goods_product_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        comp_us_net_rev_amt,
        software_usage_type_cd,
        dv_recurring_offer_cd,
        service_product_key,
        bk_busi_svc_offer_type_name,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr1_as') }}
),

source_wi_pnl_rev_cost_msr3_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        fiscal_year_number,
        fiscal_month_name,
        rev_measure_trans_type_cd,
        sales_territory_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        bk_business_unit_id,
        bk_technology_group_id,
        product_classification,
        int_sub_be,
        int_sub_be_descr,
        int_be,
        int_be_descr,
        business_group_descr,
        ext_sub_be,
        ext_sub_be_descr,
        ext_be,
        ext_be_descr,
        goods_product_key,
        tss_rev,
        software_usage_type_cd,
        service_product_key,
        bk_busi_svc_offer_type_name,
        divestiture_flg,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr3_as') }}
),

source_wi_pnl_rev_cost_msr2_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        sales_territory_key,
        goods_product_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        comp_us_net_rev_amt,
        software_usage_type_cd,
        service_product_key,
        bk_busi_svc_offer_type_name,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr2_as') }}
),

source_wi_pnl_rev_cost_msr4_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        fiscal_year_number,
        fiscal_month_name,
        rev_measure_trans_type_cd,
        sales_territory_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        bk_business_unit_id,
        bk_technology_group_id,
        product_classification,
        int_sub_be,
        int_sub_be_descr,
        int_be,
        int_be_descr,
        business_group_descr,
        ext_sub_be,
        ext_sub_be_descr,
        ext_be,
        ext_be_descr,
        goods_product_key,
        tss_rev,
        software_usage_type_cd,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name,
        service_product_key,
        divestiture_flg,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr4_as') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        fiscal_year_number,
        fiscal_month_name,
        rev_measure_trans_type_cd,
        sales_territory_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        bk_business_unit_id,
        bk_technology_group_id,
        product_classification,
        int_sub_be,
        int_sub_be_descr,
        int_be,
        int_be_descr,
        business_group_descr,
        ext_sub_be,
        ext_sub_be_descr,
        ext_be,
        ext_be_descr,
        goods_product_key,
        tss_rev,
        software_usage_type_cd,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name,
        service_product_key,
        divestiture_flg,
        dv_gsp_or_cx_product
    FROM source_wi_pnl_rev_cost_msr4_as
)

SELECT * FROM final