{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cancelled_sol', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CANCELLED_SOL',
        'target_table': 'EX_UO_OE_ORDER_LINES_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.253080+00:00'
    }
) }}

WITH 

source_ex_cg1_oe_ord_lines_hist AS (
    SELECT
        line_id,
        ship_set_id,
        reason_code,
        line_number,
        hist_creation_date,
        hist_created_by,
        latest_cancelled_quantity,
        source_commit_time,
        global_name,
        cancelled_flag,
        cancelled_quantity,
        created_by,
        creation_date,
        header_id,
        hist_comments,
        hist_type_code,
        last_updated_by,
        last_update_date,
        line_category_code,
        line_set_id,
        line_type_id,
        link_to_line_id,
        ordered_quantity,
        request_date,
        request_id,
        return_reason_code,
        salesrep_id,
        shipment_number,
        org_id,
        exception_type
    FROM {{ source('raw', 'ex_cg1_oe_ord_lines_hist') }}
),

source_st_cg1_oe_ord_lines_hist AS (
    SELECT
        line_id,
        ship_set_id,
        reason_code,
        line_number,
        hist_creation_date,
        hist_created_by,
        latest_cancelled_quantity,
        source_commit_time,
        global_name,
        cancelled_flag,
        cancelled_quantity,
        created_by,
        creation_date,
        header_id,
        hist_comments,
        hist_type_code,
        last_updated_by,
        last_update_date,
        line_category_code,
        line_set_id,
        line_type_id,
        link_to_line_id,
        ordered_quantity,
        request_date,
        request_id,
        return_reason_code,
        salesrep_id,
        shipment_number,
        org_id
    FROM {{ source('raw', 'st_cg1_oe_ord_lines_hist') }}
),

source_sm_sales_order_line AS (
    SELECT
        sales_order_line_key,
        ss_code,
        sk_so_line_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line') }}
),

source_st_uo_oe_order_lines_history AS (
    SELECT
        batch_id,
        ges_pk_id,
        cancelled_flag,
        cancelled_quantity,
        header_id,
        hist_created_by,
        hist_creation_date,
        hist_type_code,
        request_id,
        latest_cancelled_quantity,
        line_id,
        line_number,
        reason_code,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_oe_order_lines_history') }}
),

source_ex_uo_oe_order_lines_history AS (
    SELECT
        batch_id,
        ges_pk_id,
        cancelled_flag,
        cancelled_quantity,
        header_id,
        hist_created_by,
        hist_creation_date,
        hist_type_code,
        request_id,
        latest_cancelled_quantity,
        line_id,
        line_number,
        reason_code,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_uo_oe_order_lines_history') }}
),

source_ex_bo_oe_order_lines_history AS (
    SELECT
        batch_id,
        ges_pk_id,
        cancelled_flag,
        cancelled_quantity,
        header_id,
        hist_created_by,
        hist_creation_date,
        hist_type_code,
        request_id,
        latest_cancelled_quantity,
        line_id,
        line_number,
        reason_code,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        exception_type,
        create_datetime
    FROM {{ source('raw', 'ex_bo_oe_order_lines_history') }}
),

source_st_bo_oe_order_lines_history AS (
    SELECT
        batch_id,
        ges_pk_id,
        cancelled_flag,
        cancelled_quantity,
        header_id,
        hist_created_by,
        hist_creation_date,
        hist_type_code,
        request_id,
        latest_cancelled_quantity,
        line_id,
        line_number,
        reason_code,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_bo_oe_order_lines_history') }}
),

transformed_exp_w_cancelled_sol AS (
    SELECT
    sales_order_line_key,
    line_number,
    line_id,
    sol_cancel_reason_code,
    sol_cancelled_by_int,
    sol_cancelled_datetime,
    sol_cancelled_quantity,
    sol_order_quantity,
    ss_code,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime,
    edw_observation_datetime,
    action_code,
    dml_type,
    sales_order_key,
    ship_set_number_int,
    IFF(NOT ISNULL(SOL_CANCEL_REASON_CODE) AND NOT ISNULL(SOL_CANCELLED_DATETIME),'NE','NN') AS error_check,
    'UNKNOWN' AS bk_so_line_cancel_reason_cd
    FROM source_st_bo_oe_order_lines_history
),

filtered_fil_w_cancelled_sol AS (
    SELECT *
    FROM transformed_exp_w_cancelled_sol
    WHERE ERROR_CHECK = 'NE'
),

filtered_fil_ex_uo_oe_order_lines_history AS (
    SELECT *
    FROM filtered_fil_w_cancelled_sol
    WHERE ERROR_CHECK = 'NN'
),

transformed_exp_w_bo_cancelled_sol AS (
    SELECT
    sales_order_line_key,
    bk_so_line_number,
    line_id,
    reason_code,
    hist_created_by,
    hist_creation_date,
    latest_cancelled_quantity,
    sol_order_quantity,
    ss_code,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime,
    edw_observation_datetime,
    action_code,
    dml_type,
    sales_order_key,
    ship_set_number_int,
    IFF(NOT ISNULL(REASON_CODE) AND NOT ISNULL(HIST_CREATION_DATE) ,'NE','NN') AS error_check,
    'UNKNOWN' AS bk_so_line_cancel_reason_cd
    FROM filtered_fil_ex_uo_oe_order_lines_history
),

filtered_fil_w_bo_cancelled_sol AS (
    SELECT *
    FROM transformed_exp_w_bo_cancelled_sol
    WHERE ERROR_CHECK='NE'
),

transformed_exp_w_cg_cancelled_sol AS (
    SELECT
    sales_order_line_key,
    line_number,
    line_id,
    sol_cancel_reason_code,
    sol_cancelled_by_int,
    sol_cancelled_datetime,
    sol_cancelled_quantity,
    sales_order_key,
    bk_ship_set_num_int,
    sol_order_quantity,
    ss_code,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime,
    edw_observation_datetime,
    action_code,
    dml_type,
    ship_set_id,
    source_commit_time,
    global_name,
    hist_type_code,
    org_id,
    IFF(NOT ISNULL(SOL_CANCEL_REASON_CODE) AND NOT ISNULL(SOL_CANCELLED_DATETIME) ,'NE','NN') AS error_check,
    'UNKNOWN' AS bk_so_line_cancel_reason_cd
    FROM filtered_fil_w_bo_cancelled_sol
),

filtered_fil_w_cg_cancelled_sol AS (
    SELECT *
    FROM transformed_exp_w_cg_cancelled_sol
    WHERE ERROR_CHECK='NE'
),

filtered_fil_ex_cg_oe_ord_lines_hist AS (
    SELECT *
    FROM filtered_fil_w_cg_cancelled_sol
    WHERE ERROR_CHECK='NN'
),

final AS (
    SELECT
        batch_id,
        ges_pk_id,
        cancelled_flag,
        cancelled_quantity,
        header_id,
        hist_created_by,
        hist_creation_date,
        hist_type_code,
        request_id,
        latest_cancelled_quantity,
        line_id,
        line_number,
        reason_code,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime,
        exception_type
    FROM filtered_fil_ex_cg_oe_ord_lines_hist
)

SELECT * FROM final