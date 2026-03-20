{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_cm_item_reorder_point', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_CM_ITEM_REORDER_POINT',
        'target_table': 'N_CM_ITEM_REORDER_POINT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.594159+00:00'
    }
) }}

WITH 

source_n_cm_item_reorder_point_tv AS (
    SELECT
        reorder_point_qty,
        additional_reorder_point_qty,
        item_key,
        minimum_order_qty,
        inventory_orgn_name_key,
        minimum_package_qty,
        statistical_inv_sizing_qty,
        ct2r_days_cnt,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_cm_item_reorder_point_tv') }}
),

final AS (
    SELECT
        reorder_point_qty,
        additional_reorder_point_qty,
        item_key,
        minimum_order_qty,
        inventory_orgn_name_key,
        minimum_package_qty,
        statistical_inv_sizing_qty,
        ct2r_days_cnt,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_cm_item_reorder_point_tv
)

SELECT * FROM final