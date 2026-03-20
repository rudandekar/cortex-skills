{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL',
        'target_table': 'WI_AE_DISTI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.418341+00:00'
    }
) }}

WITH 

source_wi_ae_disti_l3 AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        record_type,
        sales_territory_key,
        iso_country_name,
        iso_country_code,
        ext_theater_name,
        external_geo,
        internal_geo,
        sfh_theater,
        sfh_sub_theater,
        subsales_name,
        sales_coverage_code,
        comp_us_net_rev_amt
    FROM {{ source('raw', 'wi_ae_disti_l3') }}
),

source_wi_ae_disti AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        record_type,
        sales_territory_key,
        iso_country_name,
        iso_country_code,
        ext_theater_name,
        external_geo,
        internal_geo,
        sfh_theater,
        sfh_sub_theater,
        subsales_name,
        sales_coverage_code,
        comp_us_net_rev_amt
    FROM {{ source('raw', 'wi_ae_disti') }}
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
        sales_territory_key,
        iso_country_name,
        iso_country_code,
        ext_theater_name,
        external_geo,
        internal_geo,
        sfh_theater,
        sfh_sub_theater,
        subsales_name,
        sales_coverage_code,
        comp_us_net_rev_amt
    FROM transformed_param_expression
)

SELECT * FROM final