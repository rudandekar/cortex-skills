{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_AS',
        'target_table': 'WI_SALES_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.016525+00:00'
    }
) }}

WITH 

source_wi_sales_as AS (
    SELECT
        bk_fiscal_year_mth_number_int,
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
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        comp_us_net_rev_amt
    FROM {{ source('raw', 'wi_sales_as') }}
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
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        comp_us_net_rev_amt
    FROM source_wi_table_name_param_pnl_as
)

SELECT * FROM final