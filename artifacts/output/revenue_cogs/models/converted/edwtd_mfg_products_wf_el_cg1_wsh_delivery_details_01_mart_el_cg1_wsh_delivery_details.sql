{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_wsh_delivery_details', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_WSH_DELIVERY_DETAILS',
        'target_table': 'EL_CG1_WSH_DELIVERY_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.189259+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_delivery_details AS (
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
    FROM {{ source('raw', 'st_cg1_wsh_delivery_details') }}
),

final AS (
    SELECT
        delivery_detail_id,
        source_code,
        source_line_id,
        source_header_id,
        ship_set_id,
        attribute3,
        attribute8,
        attribute11,
        ship_method_code
    FROM source_st_cg1_wsh_delivery_details
)

SELECT * FROM final