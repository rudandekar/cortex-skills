{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_order_line_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_ORDER_LINE_TYPE',
        'target_table': 'N_PURCHASE_ORDER_LINE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.619155+00:00'
    }
) }}

WITH 

source_w_purchase_order_line_type AS (
    SELECT
        bk_purchase_order_line_type_cd,
        purchase_basis_cd,
        purchase_order_line_type_descr,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_purchase_order_line_type') }}
),

final AS (
    SELECT
        bk_purchase_order_line_type_cd,
        purchase_basis_cd,
        purchase_order_line_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_purchase_order_line_type
)

SELECT * FROM final