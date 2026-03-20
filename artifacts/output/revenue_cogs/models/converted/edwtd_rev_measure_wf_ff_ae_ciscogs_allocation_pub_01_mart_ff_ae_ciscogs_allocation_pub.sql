{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ae_ciscogs_allocation_pub', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_AE_CISCOGS_ALLOCATION_PUB',
        'target_table': 'FF_AE_CISCOGS_ALLOCATION_PUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.451104+00:00'
    }
) }}

WITH 

source_ae_ciscogs_allocation_pub AS (
    SELECT
        fiscal_month_id,
        node_level05_value,
        node_level06_value,
        node_level07_value,
        node_level08_value,
        node_level09_value,
        node_level10_value,
        node_level11_value,
        node_level12_value,
        node_level13_value,
        node_level14_value,
        node_level15_value,
        dept_number,
        sales_territory_code,
        product_family,
        product_id,
        pnl_line_item_name,
        sub_measure_key,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'ae_ciscogs_allocation_pub') }}
),

final AS (
    SELECT
        fiscal_month_id,
        node_level05_value,
        node_level06_value,
        node_level07_value,
        node_level08_value,
        node_level09_value,
        node_level10_value,
        node_level11_value,
        node_level12_value,
        node_level13_value,
        node_level14_value,
        node_level15_value,
        dept_number,
        sales_territory_code,
        product_family,
        product_id,
        pnl_line_item_name,
        sub_measure_key,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        ges_update_date
    FROM source_ae_ciscogs_allocation_pub
)

SELECT * FROM final