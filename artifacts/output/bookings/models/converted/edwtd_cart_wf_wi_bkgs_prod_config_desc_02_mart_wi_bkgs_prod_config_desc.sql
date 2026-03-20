{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkgs_prod_config_desc', 'batch', 'edwtd_cart'],
    meta={
        'source_workflow': 'wf_m_WI_BKGS_PROD_CONFIG_DESC',
        'target_table': 'WI_BKGS_PROD_CONFIG_DESC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.211239+00:00'
    }
) }}

WITH 

source_wi_product_configuration AS (
    SELECT
        product_configuration_desc,
        major_product_key,
        major_sales_order_line_key,
        major_product_id,
        minor_product_key,
        rank_number
    FROM {{ source('raw', 'wi_product_configuration') }}
),

final AS (
    SELECT
        product_configuration_desc,
        major_product_key,
        major_sales_order_line_key,
        minor_product_key,
        major_product_id,
        rank_number
    FROM source_wi_product_configuration
)

SELECT * FROM final