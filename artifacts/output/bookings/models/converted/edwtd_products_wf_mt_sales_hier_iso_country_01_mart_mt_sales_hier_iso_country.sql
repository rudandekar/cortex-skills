{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sales_hier_iso_country', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_MT_SALES_HIER_ISO_COUNTRY',
        'target_table': 'MT_SALES_HIER_ISO_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.970682+00:00'
    }
) }}

WITH 

source_mt_sales_hier_iso_country AS (
    SELECT
        sales_territory_key,
        l6_sales_territory_name_cd,
        sales_subcoverage_cd,
        iso_country_cd,
        dd_iso_country_name
    FROM {{ source('raw', 'mt_sales_hier_iso_country') }}
),

final AS (
    SELECT
        sales_territory_key,
        l6_sales_territory_name_cd,
        sales_subcoverage_cd,
        iso_country_cd,
        dd_iso_country_name
    FROM source_mt_sales_hier_iso_country
)

SELECT * FROM final