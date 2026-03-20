{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_assgnment_dels', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_ASSGNMENT_DELS',
        'target_table': 'ST_BO_CSM_LINE_SALE_CRDTS_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.801349+00:00'
    }
) }}

WITH 

source_ex_bo_csm_line_sale_crdts_dels AS (
    SELECT
        line_seq_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date,
        exception_type
    FROM {{ source('raw', 'ex_bo_csm_line_sale_crdts_dels') }}
),

source_st_uo_csm_line_sale_crdts_dels AS (
    SELECT
        line_seq_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_uo_csm_line_sale_crdts_dels') }}
),

source_st_bo_csm_line_sale_crdts_dels AS (
    SELECT
        line_seq_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_bo_csm_line_sale_crdts_dels') }}
),

source_ex_uo_csm_line_sale_crdts_dels AS (
    SELECT
        line_seq_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date,
        exception_type
    FROM {{ source('raw', 'ex_uo_csm_line_sale_crdts_dels') }}
),

source_sm_sales_credit_assignment AS (
    SELECT
        sales_credit_asgn_applied_key,
        sk_line_sequence_id_int,
        ss_code,
        sk_sc_agent_id_int,
        edw_create_user,
        edw_create_dtm,
        dv_nrt_batch_flag
    FROM {{ source('raw', 'sm_sales_credit_assignment') }}
),

final AS (
    SELECT
        line_seq_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date,
        source_commit_time
    FROM source_sm_sales_credit_assignment
)

SELECT * FROM final