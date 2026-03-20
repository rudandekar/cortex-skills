{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gpf_bts_tan_inventory_item', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GPF_BTS_TAN_INVENTORY_ITEM',
        'target_table': 'W_GPF_BTS_TAN_INVENTORY_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.774046+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_bts_extr_tan AS (
    SELECT
        tan_id,
        tan_organization_code,
        on_hand,
        unit_cost,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_sp_bts_extr_tan') }}
),

source_w_gpf_bts_tan_inventory_item AS (
    SELECT
        inventory_orgn_name_key,
        tan_item_key,
        on_hand_qty,
        unit_usd_cost,
        publication_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gpf_bts_tan_inventory_item') }}
),

final AS (
    SELECT
        inventory_orgn_name_key,
        tan_item_key,
        on_hand_qty,
        unit_usd_cost,
        publication_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_gpf_bts_tan_inventory_item
)

SELECT * FROM final