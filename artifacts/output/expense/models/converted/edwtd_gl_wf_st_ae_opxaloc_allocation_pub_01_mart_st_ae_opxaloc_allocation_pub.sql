{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_opxaloc_allocation_pub', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_AE_OPXALOC_ALLOCATION_PUB',
        'target_table': 'ST_AE_OPXALOC_ALLOCATION_PUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.707626+00:00'
    }
) }}

WITH 

source_ff_ae_opxaloc_allocation_pub AS (
    SELECT
        fiscal_month_id,
        measure_id,
        measure_name,
        sub_measure_key,
        sales_territory_code,
        product_family,
        product_id,
        company_cd,
        department_cd,
        allocation_percentage,
        node_level01_value_code,
        node_level02_value_code,
        node_level03_value_code,
        node_level04_value_code,
        node_level05_value_code,
        node_level06_value_code,
        node_level07_value_code,
        node_level08_value_code,
        node_level09_value_code,
        node_level10_value_code,
        node_level11_value_code,
        node_level12_value_code,
        node_level13_value_code,
        node_level14_value_code,
        node_level15_value_code,
        pnl_line_item,
        pnl_measure_name,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'ff_ae_opxaloc_allocation_pub') }}
),

final AS (
    SELECT
        fiscal_month_id,
        measure_id,
        measure_name,
        sub_measure_key,
        sales_territory_code,
        product_family,
        product_id,
        company_cd,
        department_cd,
        allocation_percentage,
        node_level01_value_code,
        node_level02_value_code,
        node_level03_value_code,
        node_level04_value_code,
        node_level05_value_code,
        node_level06_value_code,
        node_level07_value_code,
        node_level08_value_code,
        node_level09_value_code,
        node_level10_value_code,
        node_level11_value_code,
        node_level12_value_code,
        node_level13_value_code,
        node_level14_value_code,
        node_level15_value_code,
        pnl_line_item,
        pnl_measure_name,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM source_ff_ae_opxaloc_allocation_pub
)

SELECT * FROM final