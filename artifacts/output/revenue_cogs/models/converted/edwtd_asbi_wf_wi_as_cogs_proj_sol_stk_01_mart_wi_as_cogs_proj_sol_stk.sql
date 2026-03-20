{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_proj_sol_stk', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PROJ_SOL_STK',
        'target_table': 'WI_AS_COGS_PROJ_SOL_STK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.528767+00:00'
    }
) }}

WITH 

source_wi_as_cogs_proj_sol_stk AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        project_cd,
        pnl_line_item,
        bk_sales_territory_key,
        l6_sales_territory_descr,
        l3_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        external_theater,
        allocated_cost
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_stk') }}
),

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
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
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        external_theater,
        allocated_cost
    FROM source_wi_table_name_param_pnl_as
)

SELECT * FROM final