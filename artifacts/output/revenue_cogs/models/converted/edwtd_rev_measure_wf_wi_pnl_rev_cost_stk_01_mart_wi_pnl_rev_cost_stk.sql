{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_stk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_STK',
        'target_table': 'WI_PNL_REV_COST_STK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.209147+00:00'
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
    FROM source_wi_table_name_param
)

SELECT * FROM final