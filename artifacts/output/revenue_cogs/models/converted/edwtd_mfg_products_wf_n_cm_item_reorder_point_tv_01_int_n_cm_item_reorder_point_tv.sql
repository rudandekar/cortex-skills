{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_cm_item_reorder_point_tv', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_CM_ITEM_REORDER_POINT_TV',
        'target_table': 'N_CM_ITEM_REORDER_POINT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.158462+00:00'
    }
) }}

WITH 

source_w_cm_item_reorder_point AS (
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
    FROM {{ source('raw', 'w_cm_item_reorder_point') }}
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
        edw_update_dtm
    FROM source_w_cm_item_reorder_point
)

SELECT * FROM final