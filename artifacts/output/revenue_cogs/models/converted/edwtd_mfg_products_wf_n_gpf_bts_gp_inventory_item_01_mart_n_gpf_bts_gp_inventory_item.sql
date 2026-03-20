{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gpf_bts_gp_inventory_item', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_GPF_BTS_GP_INVENTORY_ITEM',
        'target_table': 'N_GPF_BTS_GP_INVENTORY_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.349837+00:00'
    }
) }}

WITH 

source_w_gpf_bts_gp_inventory_item AS (
    SELECT
        inventory_orgn_name_key,
        goods_product_key,
        on_hand_qty,
        unit_usd_cost,
        end_of_sale_dt,
        publication_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gpf_bts_gp_inventory_item') }}
),

final AS (
    SELECT
        inventory_orgn_name_key,
        goods_product_key,
        on_hand_qty,
        unit_usd_cost,
        end_of_sale_dt,
        publication_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_gpf_bts_gp_inventory_item
)

SELECT * FROM final