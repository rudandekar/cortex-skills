{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_line_error_msg', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_LINE_ERROR_MSG',
        'target_table': 'N_SALES_ORDER_LINE_ERROR_MSG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.757113+00:00'
    }
) }}

WITH 

source_n_sales_order_line_error_msg AS (
    SELECT
        sales_order_line_error_msg_key,
        sk_entitlement_error_id_int,
        sk_line_error_id_int,
        sk_batch_id_int,
        ss_cd,
        created_dtm,
        dv_created_dt,
        error_message_txt,
        sales_order_line_key,
        sales_order_key,
        error_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sales_order_line_error_msg') }}
),

final AS (
    SELECT
        sales_order_line_error_msg_key,
        sk_entitlement_error_id_int,
        sk_line_error_id_int,
        sk_batch_id_int,
        ss_cd,
        created_dtm,
        dv_created_dt,
        error_message_txt,
        sales_order_line_key,
        sales_order_key,
        error_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sales_order_line_error_msg
)

SELECT * FROM final