{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_sales_order_line_error_msg', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_LINE_ERROR_MSG',
        'target_table': 'EX_CG1_XXGCO_SRVC_ERRS_IN_INTF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.015506+00:00'
    }
) }}

WITH 

source_ex_cg1_xxgco_srvc_errs_in_intf AS (
    SELECT
        line_int_sequence_id,
        header_id,
        line_id,
        error_code,
        error_message,
        status,
        referenceid,
        source_control_id,
        sourcesystem,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        src_batch_id,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM {{ source('raw', 'ex_cg1_xxgco_srvc_errs_in_intf') }}
),

source_w_sales_order_line_error_msg AS (
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
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_line_error_msg') }}
),

source_ex_bo_xxcca_svc_ctrt_line_errs AS (
    SELECT
        line_error_id,
        line_id,
        error_message_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        valid_flag,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM {{ source('raw', 'ex_bo_xxcca_svc_ctrt_line_errs') }}
),

source_ex_uo_xxcca_svc_ctrt_line_errs AS (
    SELECT
        line_error_id,
        line_id,
        error_message_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        valid_flag,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM {{ source('raw', 'ex_uo_xxcca_svc_ctrt_line_errs') }}
),

final AS (
    SELECT
        line_int_sequence_id,
        header_id,
        line_id,
        error_code,
        error_message,
        status,
        referenceid,
        source_control_id,
        sourcesystem,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        src_batch_id,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM source_ex_uo_xxcca_svc_ctrt_line_errs
)

SELECT * FROM final