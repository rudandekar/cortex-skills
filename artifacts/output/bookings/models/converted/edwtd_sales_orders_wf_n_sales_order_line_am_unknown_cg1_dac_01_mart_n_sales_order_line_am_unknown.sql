{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_line_am_unknown_cg1_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_LINE_AM_UNKNOWN_CG1_DAC',
        'target_table': 'N_SALES_ORDER_LINE_AM_UNKNOWN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.546811+00:00'
    }
) }}

WITH 

source_w_sales_order_line_am_unknown AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_line_am_unknown') }}
),

final AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sales_order_line_am_unknown
)

SELECT * FROM final