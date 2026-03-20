{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_em_item_master', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_EM_ITEM_MASTER',
        'target_table': 'FF_EM_ITEM_MASTER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.458004+00:00'
    }
) }}

WITH 

source_em_item_master AS (
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
        creation_date
    FROM {{ source('raw', 'em_item_master') }}
),

transformed_exp_em_item_master AS (
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
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_em_item_master
),

final AS (
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
    FROM transformed_exp_em_item_master
)

SELECT * FROM final