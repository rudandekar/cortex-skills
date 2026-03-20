{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_assgnment', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_ASSGNMENT',
        'target_table': 'W_SALES_CREDIT_ASSGNMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.582655+00:00'
    }
) }}

WITH 

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

source_st_uo_csm_line_sales_credits AS (
    SELECT
        line_seq_id,
        header_seq_id,
        rebate_percentage_id,
        rebate_per_unit,
        salesrep_id,
        sales_credit_type_id,
        source_header_id,
        source_line_id,
        source_type,
        split_percent,
        territory_id,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        batch_id,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_csm_line_sales_credits') }}
),

final AS (
    SELECT
        sales_credit_asgn_key,
        sales_order_line_key,
        bk_sales_rep_number,
        bk_sales_credit_type_code,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        sales_commission_percentage,
        sca_source_type_code,
        sales_territory_key,
        ss_code,
        sk_line_seq_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sk_sc_agent_id_int,
        action_code,
        dml_type
    FROM source_st_uo_csm_line_sales_credits
)

SELECT * FROM final