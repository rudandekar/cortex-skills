{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_sets_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_SETS_DAC',
        'target_table': 'ST_CG1_OE_SETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.102959+00:00'
    }
) }}

WITH 

source_cg1_oe_sets AS (
    SELECT
        set_id,
        set_name,
        set_type,
        header_id,
        ship_from_org_id,
        ship_to_org_id,
        schedule_ship_date,
        schedule_arrival_date,
        freight_carrier_code,
        shipping_method_code,
        shipment_priority_code,
        set_status,
        created_by,
        creation_date,
        updated_by,
        update_date,
        update_login,
        inventory_item_id,
        ordered_quantity_uom,
        line_type_id,
        ship_tolerance_above,
        ship_tolerance_below,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_oe_sets') }}
),

final AS (
    SELECT
        set_id,
        set_name,
        set_type,
        header_id,
        ship_from_org_id,
        ship_to_org_id,
        schedule_ship_date,
        schedule_arrival_date,
        freight_carrier_code,
        shipping_method_code,
        shipment_priority_code,
        set_status,
        created_by,
        creation_date,
        updated_by,
        update_date,
        update_login,
        inventory_item_id,
        ordered_quantity_uom,
        line_type_id,
        ship_tolerance_above,
        ship_tolerance_below,
        source_commit_time,
        global_name
    FROM source_cg1_oe_sets
)

SELECT * FROM final