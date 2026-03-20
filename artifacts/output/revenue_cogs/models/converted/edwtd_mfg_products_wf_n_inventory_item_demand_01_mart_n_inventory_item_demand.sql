{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_inventory_item_demand', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_INVENTORY_ITEM_DEMAND',
        'target_table': 'N_INVENTORY_ITEM_DEMAND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.860395+00:00'
    }
) }}

WITH 

source_w_inventory_item_demand AS (
    SELECT
        inventory_item_demand_key,
        bk_inventory_organization_key,
        bk_item_key,
        bk_inventory_item_demand_dt,
        with_parent_role,
        reservation_type_cd,
        inventory_item_demand_qty,
        inv_item_demand_completed_qty,
        demand_source_role,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_inventory_item_demand') }}
),

final AS (
    SELECT
        inventory_item_demand_key,
        bk_inventory_organization_key,
        bk_item_key,
        bk_inventory_item_demand_dt,
        with_parent_role,
        reservation_type_cd,
        inventory_item_demand_qty,
        inv_item_demand_completed_qty,
        demand_source_role,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_inventory_item_demand
)

SELECT * FROM final