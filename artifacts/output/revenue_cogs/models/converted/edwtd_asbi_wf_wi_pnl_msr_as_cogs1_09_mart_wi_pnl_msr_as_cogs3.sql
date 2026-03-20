{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL_AS',
        'target_table': 'WI_PNL_MSR_AS_COGS3',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.841496+00:00'
    }
) }}

WITH 

source_wi_pnl_msr_as_cogs2 AS (
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
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg,
        l2_tms_name
    FROM {{ source('raw', 'wi_pnl_msr_as_cogs2') }}
),

source_wi_pnl_msr_as_cogs1 AS (
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
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg
    FROM {{ source('raw', 'wi_pnl_msr_as_cogs1') }}
),

source_wi_pnl_msr_as_cogs3 AS (
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
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg,
        l2_tms_name,
        internal_be_name,
        internal_be_descr,
        internal_sub_be_name,
        internal_sub_be_descr,
        business_group_descr,
        external_be_name,
        external_be_descr,
        external_sub_be_name,
        external_sub_be_descr
    FROM {{ source('raw', 'wi_pnl_msr_as_cogs3') }}
),

source_wi_mt_as_cogs AS (
    SELECT
        sales_territory_key,
        fiscal_year_month_int,
        goods_product_key,
        dv_cx_product,
        pnl_line_item_name,
        bk_as_project_cd,
        dv_comp_us_net_cogs_usd_amt,
        fiscal_year_quarter_number_int,
        dv_fiscal_quarter_id,
        fiscal_year_number_int,
        fiscal_month_name
    FROM {{ source('raw', 'wi_mt_as_cogs') }}
),

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

transformed_param_expression AS (
    SELECT
    table_name1,
    table_name2,
    setvariable('MAP_VAR1',TABLE_NAME1) AS table_value1,
    setvariable('MAP_VAR2',TABLE_NAME2) AS table_value2
    FROM source_wi_table_name_param_pnl_as
),

final AS (
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
        bk_allocated_servc_group_id,
        rev_measure_trans_type_cd,
        dv_data_source_name,
        dv_comp_us_net_cogs_usd_amt,
        divestiture_flg,
        l2_tms_name,
        internal_be_name,
        internal_be_descr,
        internal_sub_be_name,
        internal_sub_be_descr,
        business_group_descr,
        external_be_name,
        external_be_descr,
        external_sub_be_name,
        external_sub_be_descr
    FROM transformed_param_expression
)

SELECT * FROM final