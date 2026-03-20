{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_proj_sol_stk_aot', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PROJ_SOL_STK_AOT',
        'target_table': 'WI_AS_COGS_PROJ_SOL_STK_AOT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.962507+00:00'
    }
) }}

WITH 

source_wi_as_cogs_proj_sol_stk_aot AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        project_cd,
        pnl_line_item,
        bk_sales_territory_key,
        l6_sales_territory_descr,
        l3_sales_territory_descr,
        l2_sales_territory_descr,
        dv_cx_product,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        as_project_class_cd,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        external_theater,
        allocated_cost
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_stk_aot') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        project_cd,
        pnl_line_item,
        bk_sales_territory_key,
        l6_sales_territory_descr,
        l3_sales_territory_descr,
        l2_sales_territory_descr,
        dv_cx_product,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        as_project_class_cd,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        external_theater,
        allocated_cost
    FROM source_wi_as_cogs_proj_sol_stk_aot
)

SELECT * FROM final