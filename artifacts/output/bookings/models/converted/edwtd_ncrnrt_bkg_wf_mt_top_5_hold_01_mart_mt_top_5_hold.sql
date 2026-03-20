{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_top_5_hold', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_MT_TOP_5_HOLD',
        'target_table': 'MT_TOP_5_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.635748+00:00'
    }
) }}

WITH 

source_mt_top_5_hold AS (
    SELECT
        sales_order_line_key,
        so_hold_1_name,
        so_hold_1_place_dt,
        so_hold_1_placed_erp_user_name,
        so_hold_2_name,
        so_hold_2_place_dt,
        so_hold_2_placed_erp_user_name,
        so_hold_3_name,
        so_hold_3_place_dt,
        so_hold_3_placed_erp_user_name,
        so_hold_4_name,
        so_hold_4_place_dt,
        so_hold_4_placed_erp_user_name,
        so_hold_5_name,
        so_hold_5_place_dt,
        so_hold_5_placed_erp_user_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_top_5_hold') }}
),

final AS (
    SELECT
        sales_order_line_key,
        so_hold_1_name,
        so_hold_1_place_dt,
        so_hold_1_placed_erp_user_name,
        so_hold_2_name,
        so_hold_2_place_dt,
        so_hold_2_placed_erp_user_name,
        so_hold_3_name,
        so_hold_3_place_dt,
        so_hold_3_placed_erp_user_name,
        so_hold_4_name,
        so_hold_4_place_dt,
        so_hold_4_placed_erp_user_name,
        so_hold_5_name,
        so_hold_5_place_dt,
        so_hold_5_placed_erp_user_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_top_5_hold
)

SELECT * FROM final