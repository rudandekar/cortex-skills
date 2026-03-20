{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sales_order_line_error_msg', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SALES_ORDER_LINE_ERROR_MSG',
        'target_table': 'SM_SALES_ORDER_LINE_ERROR_MSG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.428556+00:00'
    }
) }}

WITH 

source_sm_sales_order_line_error_msg AS (
    SELECT
        sales_order_line_error_msg_key,
        sk_entitlement_error_id_int,
        sk_line_error_id_int,
        sk_batch_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sales_order_line_error_msg') }}
),

final AS (
    SELECT
        sales_order_line_error_msg_key,
        sk_entitlement_error_id_int,
        sk_line_error_id_int,
        sk_batch_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_sales_order_line_error_msg
)

SELECT * FROM final