{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_price_list', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_PRICE_LIST',
        'target_table': 'ST_INT_RAW_PRICE_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.941141+00:00'
    }
) }}

WITH 

source_ff_int_raw_price_list AS (
    SELECT
        price_list_id,
        erp_price_list_id,
        short_desc,
        erp_price_list_name,
        title_1,
        currency_code
    FROM {{ source('raw', 'ff_int_raw_price_list') }}
),

final AS (
    SELECT
        price_list_id,
        erp_price_list_id,
        short_desc,
        erp_price_list_name,
        title_1,
        currency_code
    FROM source_ff_int_raw_price_list
)

SELECT * FROM final