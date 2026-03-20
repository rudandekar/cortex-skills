{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cm_item_supply_commit', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_CM_ITEM_SUPPLY_COMMIT',
        'target_table': 'N_CM_ITEM_SUPPLY_COMMIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.227401+00:00'
    }
) }}

WITH 

source_w_cm_item_supply_commit AS (
    SELECT
        item_key,
        inventory_orgn_name_key,
        bk_supply_commit_planned_dt,
        supply_commit_start_dt,
        supply_commit_end_dt,
        commit_message_txt,
        committed_item_qty,
        pip_inventory_org_key,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cm_item_supply_commit') }}
),

final AS (
    SELECT
        item_key,
        inventory_orgn_name_key,
        bk_supply_commit_planned_dt,
        supply_commit_start_dt,
        supply_commit_end_dt,
        commit_message_txt,
        committed_item_qty,
        pip_inventory_org_key,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_w_cm_item_supply_commit
)

SELECT * FROM final