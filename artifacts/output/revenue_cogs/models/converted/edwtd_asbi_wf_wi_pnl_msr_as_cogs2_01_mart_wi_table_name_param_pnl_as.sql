{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL_AS',
        'target_table': 'WI_TABLE_NAME_PARAM_PNL_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.562297+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_pnl_msr_as_cogs4 AS (
    SELECT
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        dv_fiscal_year_num,
        dv_fiscal_month_name,
        fiscal_year_quarter_number_int,
        dv_cx_product,
        pnl_line_item_name,
        goods_product_key,
        bk_as_project_cd,
        ru_bk_product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        ru_software_usage_type_cd,
        bk_business_unit_id,
        bk_technology_group_id,
        bk_prdt_allctn_clsfctn_cd,
        dv_sw_service_category_cd,
        l2_tms_name,
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg,
        internal_be_name,
        internal_be_descr,
        internal_sub_be_name,
        internal_sub_be_descr,
        business_group_descr,
        external_be_name,
        external_be_descr,
        external_sub_be_name,
        external_sub_be_descr,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name
    FROM {{ source('raw', 'wi_pnl_msr_as_cogs4') }}
),

source_wi_pnl_msr_as_cogs5 AS (
    SELECT
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        dv_fiscal_year_num,
        dv_fiscal_month_name,
        fiscal_year_quarter_number_int,
        dv_cx_product,
        pnl_line_item_name,
        goods_product_key,
        bk_as_project_cd,
        ru_bk_product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        ru_software_usage_type_cd,
        bk_business_unit_id,
        bk_technology_group_id,
        bk_prdt_allctn_clsfctn_cd,
        dv_sw_service_category_cd,
        l2_tms_name,
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg,
        internal_be_name,
        internal_be_descr,
        internal_sub_be_name,
        internal_sub_be_descr,
        business_group_descr,
        external_be_name,
        external_be_descr,
        external_sub_be_name,
        external_sub_be_descr,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name,
        l1_sales_territory_name_cd,
        l2_sales_territory_name_cd,
        l3_sales_territory_name_cd,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        sales_coverage_cd,
        sales_subcoverage_cd,
        dd_external_theater_name,
        iso_country_name,
        bk_iso_country_code,
        level03_theater_name,
        level04_theater_name,
        level05_theater_name,
        level06_theater_name
    FROM {{ source('raw', 'wi_pnl_msr_as_cogs5') }}
),

transformed_param_expression AS (
    SELECT
    table_name1,
    table_name2,
    setvariable('MAP_VAR1',TABLE_NAME1) AS table_value1,
    setvariable('MAP_VAR2',TABLE_NAME2) AS table_value2
    FROM source_wi_pnl_msr_as_cogs5
),

final AS (
    SELECT
        table_name1,
        table_name2
    FROM transformed_param_expression
)

SELECT * FROM final