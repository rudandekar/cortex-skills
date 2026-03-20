{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_inv_item_demand_so_line', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_INV_ITEM_DEMAND_SO_LINE',
        'target_table': 'N_INV_ITEM_DEMAND_SO_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.408303+00:00'
    }
) }}

WITH 

source_w_inv_item_demand_so_line AS (
    SELECT
        inventory_item_demand_key,
        sales_order_line_key,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_inv_item_demand_so_line') }}
),

final AS (
    SELECT
        inventory_item_demand_key,
        sales_order_line_key,
        sk_demand_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_inv_item_demand_so_line
)

SELECT * FROM final