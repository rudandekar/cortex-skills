{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cm_pull_in_message', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CM_PULL_IN_MESSAGE',
        'target_table': 'N_CM_PULL_IN_MESSAGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.742554+00:00'
    }
) }}

WITH 

source_w_cm_pull_in_message AS (
    SELECT
        cm_pull_in_message_key,
        shipset_value_usd_amt,
        sk_message_id,
        proposed_scheduled_ship_dt,
        proposed_promise_dt,
        pull_in_reason_cd,
        cisco_detail_reason_cd,
        pull_in_comment,
        cm_employee_name,
        process_status_cd,
        b2b_error_cd,
        b2b_error_message_txt,
        transmission_dtm,
        dv_transmission_dt,
        current_scheduled_ship_dt,
        current_promise_dt,
        planner_requested_ship_dt,
        target_scheduled_ship_dt,
        target_promise_dt,
        shpmnt_secondary_priority_cd,
        shipset_status_cd,
        sales_order_line_num_int,
        sales_order_num_int,
        top_model_sol_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        consolidation_flg,
        consolidated_set_id_int,
        intermediate_promise_ship_dt,
        intermediate_promise_dlvry_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cm_pull_in_message') }}
),

final AS (
    SELECT
        cm_pull_in_message_key,
        shipset_value_usd_amt,
        sk_message_id,
        proposed_scheduled_ship_dt,
        proposed_promise_dt,
        pull_in_reason_cd,
        cisco_detail_reason_cd,
        pull_in_comment,
        cm_employee_name,
        process_status_cd,
        b2b_error_cd,
        b2b_error_message_txt,
        transmission_dtm,
        dv_transmission_dt,
        current_scheduled_ship_dt,
        current_promise_dt,
        planner_requested_ship_dt,
        target_scheduled_ship_dt,
        target_promise_dt,
        shpmnt_secondary_priority_cd,
        shipset_status_cd,
        sales_order_line_num_int,
        sales_order_num_int,
        top_model_sol_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        consolidation_flg,
        consolidated_set_id_int,
        intermediate_promise_ship_dt,
        intermediate_promise_dlvry_dt
    FROM source_w_cm_pull_in_message
)

SELECT * FROM final