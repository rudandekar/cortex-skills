{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ts_smt', 'batch', 'edwtd_smt'],
    meta={
        'source_workflow': 'wf_m_ST_TS_SMT',
        'target_table': 'st_ts_smt',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.673594+00:00'
    }
) }}

WITH 

source_sales_motion_timing11 AS (
    SELECT
        sales_order_line_key,
        sales_motion_timing_cd,
        batch_number
    FROM {{ source('raw', 'sales_motion_timing11') }}
),

transformed_exp_smt AS (
    SELECT
    sales_order_line_key,
    sales_motion_timing_cd,
    batch_number,
    CURRENT_TIMESTAMP() AS last_update_date
    FROM source_sales_motion_timing11
),

final AS (
    SELECT
        sales_order_line_key,
        sales_motion_timing_cd,
        batch_number,
        last_update_date
    FROM transformed_exp_smt
)

SELECT * FROM final