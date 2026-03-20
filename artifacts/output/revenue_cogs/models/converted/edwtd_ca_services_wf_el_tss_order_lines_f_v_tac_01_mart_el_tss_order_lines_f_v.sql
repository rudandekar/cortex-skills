{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_order_lines_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_ORDER_LINES_F_V',
        'target_table': 'EL_TSS_ORDER_LINES_F_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.401411+00:00'
    }
) }}

WITH 

source_st_tss_order_lines_f_v_tac AS (
    SELECT
        bl_line_key,
        std_cost_ext_usd_amt,
        shipped_quantity,
        gbl_list_price_ext_usd_amt,
        header_id,
        actual_ship_date_calendar_key,
        product_key,
        inventory_item_id,
        line_type_lkp_key,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        actual_ship_date,
        labor_line_flag
    FROM {{ source('raw', 'st_tss_order_lines_f_v_tac') }}
),

final AS (
    SELECT
        bl_line_key,
        std_cost_ext_usd_amt,
        shipped_quantity,
        gbl_list_price_ext_usd_amt,
        header_id,
        actual_ship_date_calendar_key,
        product_key,
        inventory_item_id,
        line_type_lkp_key,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        actual_ship_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        labor_line_flag
    FROM source_st_tss_order_lines_f_v_tac
)

SELECT * FROM final