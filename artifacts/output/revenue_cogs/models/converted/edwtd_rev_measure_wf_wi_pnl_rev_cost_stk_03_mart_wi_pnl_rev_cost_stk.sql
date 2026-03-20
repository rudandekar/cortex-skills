{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL',
        'target_table': 'WI_PNL_REV_COST_STK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.736344+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_stk AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        record_type,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        iso_country_name,
        iso_country_code,
        ext_theater_name,
        external_geo,
        internal_geo,
        sfh_theater,
        sfh_sub_theater,
        subsales_name,
        sales_coverage_code,
        total_rev,
        attr_rev,
        attr_alloc_pct
    FROM {{ source('raw', 'wi_pnl_rev_cost_stk') }}
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
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        record_type,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        iso_country_name,
        iso_country_code,
        ext_theater_name,
        external_geo,
        internal_geo,
        sfh_theater,
        sfh_sub_theater,
        subsales_name,
        sales_coverage_code,
        total_rev,
        attr_rev,
        attr_alloc_pct
    FROM transformed_param_expression
)

SELECT * FROM final