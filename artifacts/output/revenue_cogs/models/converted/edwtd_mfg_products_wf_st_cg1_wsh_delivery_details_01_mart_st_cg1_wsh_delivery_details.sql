{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_wsh_delivery_details', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_WSH_DELIVERY_DETAILS',
        'target_table': 'ST_CG1_WSH_DELIVERY_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.108650+00:00'
    }
) }}

WITH 

source_cg1_wsh_delivery_details AS (
    SELECT
        delivery_detail_id,
        source_code,
        source_line_id,
        source_header_id,
        inventory_item_id,
        item_description,
        ship_set_id,
        shipped_quantity,
        move_order_line_id,
        subinventory,
        released_status,
        date_requested,
        date_scheduled,
        ship_method_code,
        freight_terms_code,
        tracking_number,
        attribute1,
        attribute2,
        attribute3,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute11,
        gross_weight,
        container_name,
        picked_quantity,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'cg1_wsh_delivery_details') }}
),

final AS (
    SELECT
        delivery_detail_id,
        source_code,
        source_line_id,
        source_header_id,
        inventory_item_id,
        item_description,
        ship_set_id,
        shipped_quantity,
        move_order_line_id,
        subinventory,
        released_status,
        date_requested,
        date_scheduled,
        ship_method_code,
        freight_terms_code,
        tracking_number,
        attribute1,
        attribute2,
        attribute3,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute11,
        gross_weight,
        container_name,
        picked_quantity,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_wsh_delivery_details
)

SELECT * FROM final