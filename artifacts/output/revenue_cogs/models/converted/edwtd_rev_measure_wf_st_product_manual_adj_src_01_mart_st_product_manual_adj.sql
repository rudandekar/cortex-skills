{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_product_manual_adj_src', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_PRODUCT_MANUAL_ADJ_SRC',
        'target_table': 'ST_PRODUCT_MANUAL_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.297027+00:00'
    }
) }}

WITH 

source_ff_product_manual_adj AS (
    SELECT
        fiscal_month_id,
        fiscal_quarter_id,
        company_name,
        ext_theater,
        sales_territory_key,
        sales_l1_desc,
        sales_l2_desc,
        country_name,
        sales_subcoverage_code,
        product_id,
        product_family_id,
        adj_name,
        measure,
        measure_name,
        sub_measure_name,
        data_type,
        amount,
        ges_update_date,
        source_type
    FROM {{ source('raw', 'ff_product_manual_adj') }}
),

transformed_exptrans AS (
    SELECT
    fiscal_month_id,
    company_name,
    sales_territory_key,
    country_name,
    sales_subcoverage_code,
    product_id,
    measure_name,
    sub_measure_name,
    data_type,
    amount,
    'EXCEL' AS source_type
    FROM source_ff_product_manual_adj
),

final AS (
    SELECT
        fiscal_month_id,
        company_name,
        sales_territory_key,
        country_name,
        sales_subcoverage_code,
        product_id,
        measure_name,
        sub_measure_name,
        data_type,
        amount,
        ges_update_date,
        source_type,
        deal_id
    FROM transformed_exptrans
)

SELECT * FROM final