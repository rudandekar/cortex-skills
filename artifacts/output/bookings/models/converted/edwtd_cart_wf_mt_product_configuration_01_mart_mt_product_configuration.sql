{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_product_configuration', 'batch', 'edwtd_cart'],
    meta={
        'source_workflow': 'wf_m_MT_PRODUCT_CONFIGURATION',
        'target_table': 'MT_PRODUCT_CONFIGURATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.603534+00:00'
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

source_mt_product_configuration AS (
    SELECT
        dv_product_configuration_name,
        dv_product_configuration_desc,
        major_line_product_id,
        prod_config_seq_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_product_configuration') }}
),

final AS (
    SELECT
        dv_product_configuration_name,
        dv_product_configuration_desc,
        major_line_product_id,
        prod_config_seq_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_product_configuration
)

SELECT * FROM final