{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_csf_oe_order_lines_all', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_CSF_OE_ORDER_LINES_ALL',
        'target_table': 'ST_CSF_OE_ORDER_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.199964+00:00'
    }
) }}

WITH 

source_ff_csf_oe_order_lines_all AS (
    SELECT
        line_id,
        header_id,
        ordered_item,
        inventory_item_id,
        line_category_code,
        last_update_date,
        actual_shipment_date,
        attribute16,
        edw_create_dtm,
        shipment_number,
        flow_status_code
    FROM {{ source('raw', 'ff_csf_oe_order_lines_all') }}
),

source_oe_order_lines_all1 AS (
    SELECT
        line_id,
        header_id,
        ordered_item,
        inventory_item_id,
        line_category_code,
        last_update_date,
        actual_shipment_date,
        attribute16,
        shipment_number,
        flow_status_code
    FROM {{ source('raw', 'oe_order_lines_all1') }}
),

final AS (
    SELECT
        line_id,
        header_id,
        ordered_item,
        inventory_item_id,
        line_category_code,
        last_update_date,
        actual_shipment_date,
        attribute16,
        edw_create_dtm,
        shipment_number,
        flow_status_code
    FROM source_oe_order_lines_all1
)

SELECT * FROM final