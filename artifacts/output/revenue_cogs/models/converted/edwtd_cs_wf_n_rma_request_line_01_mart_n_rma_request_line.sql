{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rma_request_line', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_N_RMA_REQUEST_LINE',
        'target_table': 'N_RMA_REQUEST_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.985058+00:00'
    }
) }}

WITH 

source_w_rma_request_line AS (
    SELECT
        bk_awaiting_authorization_num,
        dv_rma_request_submit_dt,
        original_sales_order_line_key,
        rma_request_line_return_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rma_request_line') }}
),

final AS (
    SELECT
        bk_awaiting_authorization_num,
        dv_rma_request_submit_dt,
        original_sales_order_line_key,
        rma_request_line_return_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rma_request_line
)

SELECT * FROM final