{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_am_unknown', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_AM_UNKNOWN',
        'target_table': 'N_SALES_ORDER_AM_UNKNOWN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.532836+00:00'
    }
) }}

WITH 

source_w_sales_order_am_unknown AS (
    SELECT
        sales_order_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        so_am_unknown_004_cd
    FROM {{ source('raw', 'w_sales_order_am_unknown') }}
),

final AS (
    SELECT
        sales_order_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        so_am_unknown_004_cd
    FROM source_w_sales_order_am_unknown
)

SELECT * FROM final