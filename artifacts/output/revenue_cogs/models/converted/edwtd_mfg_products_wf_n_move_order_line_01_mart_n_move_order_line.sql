{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_move_order_line', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MOVE_ORDER_LINE',
        'target_table': 'N_MOVE_ORDER_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.756954+00:00'
    }
) }}

WITH 

source_w_move_order_line AS (
    SELECT
        move_order_line_key,
        pick_slip_num_int,
        pick_dtm,
        move_order_line_qty,
        sk_line_id_int,
        bk_move_order_request_num,
        move_order_inventory_org_key,
        item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_move_order_line') }}
),

final AS (
    SELECT
        move_order_line_key,
        pick_slip_num_int,
        pick_dtm,
        move_order_line_qty,
        sk_line_id_int,
        bk_move_order_request_num,
        move_order_inventory_org_key,
        item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_move_order_line
)

SELECT * FROM final