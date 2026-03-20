{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_inv_item_demand_with_parent', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_INV_ITEM_DEMAND_WITH_PARENT',
        'target_table': 'N_INV_ITEM_DEMAND_WITH_PARENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.997437+00:00'
    }
) }}

WITH 

source_w_inv_item_demand_with_parent AS (
    SELECT
        inventory_item_demand_key,
        parent_inv_item_demand_key,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_inv_item_demand_with_parent') }}
),

final AS (
    SELECT
        inventory_item_demand_key,
        parent_inv_item_demand_key,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_inv_item_demand_with_parent
)

SELECT * FROM final