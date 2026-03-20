{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_wk_cm_item_reorder_point', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_WK_CM_ITEM_REORDER_POINT',
        'target_table': 'W_CM_ITEM_REORDER_POINT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.906091+00:00'
    }
) }}

WITH 

source_st_em_item_master AS (
    SELECT
        item_master_id,
        partner_id,
        product_id,
        reorder_point_quantity,
        rop_safety_stock_quantity,
        single_use_kanban_quantity,
        cycle_time_to_replenish,
        minimum_order_quantity,
        minimum_package_quantity,
        last_update,
        creation_date,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_em_item_master') }}
),

transformed_exp_w_cm_item_reorder_point AS (
    SELECT
    item_key,
    inventory_orgn_name_key,
    start_tv_dt,
    end_tv_dt,
    reorder_point_qty,
    additional_reorder_point_qty,
    minimum_order_qty,
    minimum_package_qty,
    statistical_inv_sizing_qty,
    ct2r_days_cnt,
    action_code,
    edw_create_dtm,
    edw_update_dtm,
    rank_index,
    dml_type
    FROM source_st_em_item_master
),

final AS (
    SELECT
        item_key,
        inventory_orgn_name_key,
        start_tv_dt,
        end_tv_dt,
        reorder_point_qty,
        additional_reorder_point_qty,
        minimum_order_qty,
        minimum_package_qty,
        statistical_inv_sizing_qty,
        ct2r_days_cnt,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_w_cm_item_reorder_point
)

SELECT * FROM final