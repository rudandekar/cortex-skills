{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_so_oe_sets_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SO_OE_SETS_DAC',
        'target_table': 'WI_SO_OE_SETS_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.656836+00:00'
    }
) }}

WITH 

source_ex_cg1_oe_sets AS (
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
        global_name,
        exception_type
    FROM {{ source('raw', 'ex_cg1_oe_sets') }}
),

source_st_cg1_oe_sets AS (
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
    FROM {{ source('raw', 'st_cg1_oe_sets') }}
),

final AS (
    SELECT
        sales_order_line_key,
        line_id,
        ss_code,
        set_id,
        ship_set_number,
        header_id,
        create_datetime
    FROM source_st_cg1_oe_sets
)

SELECT * FROM final