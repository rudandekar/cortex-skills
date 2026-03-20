{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL',
        'target_table': 'WI_PNL_REV_COST_MSR4',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.831877+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_msr4 AS (
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
        divestiture_flg,
        dv_cx_gsp_or_offr_type_name
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr4') }}
),

source_wi_pnl_rev_cost_msr2 AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        sales_territory_key,
        dv_cx_gsp_or_offr_type_name,
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
        bundle_product_key
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr2') }}
),

source_wi_tss_service_revenue AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        dv_recurring_offer_cd,
        product_subgroup_id,
        bundle_product_key,
        dv_cx_product,
        pnl_line_item_name,
        software_service_category,
        sub_comp_us_net_rev_amt,
        sub_rev_split_pct
    FROM {{ source('raw', 'wi_tss_service_revenue') }}
),

source_wi_pnl_rev_cost_msr3 AS (
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
        divestiture_flg,
        dv_cx_gsp_or_offr_type_name,
        bundle_product_key
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr3') }}
),

source_wi_pnl_rev_cost_msr1 AS (
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
        bundle_product_key,
        dv_cx_product
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr1') }}
),

source_wi_table_name_param AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param') }}
),

transformed_param_expression AS (
    SELECT
    table_name1,
    table_name2,
    setvariable('MAP_VAR1',TABLE_NAME1) AS table_value1,
    setvariable('MAP_VAR2',TABLE_NAME2) AS table_value2
    FROM source_wi_table_name_param
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
        divestiture_flg,
        dv_cx_gsp_or_offr_type_name
    FROM transformed_param_expression
)

SELECT * FROM final