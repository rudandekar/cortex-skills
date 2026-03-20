{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_attr_prdt_new_rnwl_smt_upd', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_ATTR_PRDT_NEW_RNWL_SMT_UPD',
        'target_table': 'EL_SOL_SMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.291523+00:00'
    }
) }}

WITH 

source_st_non_ts_smt AS (
    SELECT
        batch_number,
        sales_order_line_key,
        sales_motion_timing_cd
    FROM {{ source('raw', 'st_non_ts_smt') }}
),

source_el_sol_smt AS (
    SELECT
        offer_name,
        batch_number,
        sales_order_line_key,
        sales_motion_timing_cd,
        start_tv_dt,
        end_tv_dt,
        edw_update_dtm
    FROM {{ source('raw', 'el_sol_smt') }}
),

final AS (
    SELECT
        offer_name,
        batch_number,
        sales_order_line_key,
        sales_motion_timing_cd,
        start_tv_dt,
        end_tv_dt,
        edw_update_dtm
    FROM source_el_sol_smt
)

SELECT * FROM final