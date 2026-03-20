{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_mtl_sales_orders_pre', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_MTL_SALES_ORDERS_PRE',
        'target_table': 'ST_CLOUD_MTL_SALES_ORDERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.858324+00:00'
    }
) }}

WITH 

source_pst_cloud_mtl_sales_orders AS (
    SELECT
        sales_order_id,
        sales_order_number,
        refresh_datetime,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'pst_cloud_mtl_sales_orders') }}
),

final AS (
    SELECT
        sales_order_id,
        segment1,
        refresh_datetime,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_mtl_sales_orders
)

SELECT * FROM final