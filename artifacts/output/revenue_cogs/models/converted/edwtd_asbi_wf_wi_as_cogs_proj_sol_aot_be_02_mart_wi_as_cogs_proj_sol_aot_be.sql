{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL_AS',
        'target_table': 'WI_AS_COGS_PROJ_SOL_AOT_BE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.800747+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_as_cogs_proj_sol_aot_be AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        project_cd,
        pnl_line_item,
        bk_sales_territory_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        goods_product_key,
        be_association_cd,
        dv_cx_product,
        bk_allocated_servc_group_id,
        allocated_cost
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_aot_be') }}
),

transformed_param_expression AS (
    SELECT
    table_name1,
    table_name2,
    setvariable('MAP_VAR1',TABLE_NAME1) AS table_value1,
    setvariable('MAP_VAR2',TABLE_NAME2) AS table_value2
    FROM source_wi_as_cogs_proj_sol_aot_be
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        project_cd,
        pnl_line_item,
        bk_sales_territory_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        goods_product_key,
        be_association_cd,
        dv_cx_product,
        bk_allocated_servc_group_id,
        allocated_cost
    FROM transformed_param_expression
)

SELECT * FROM final