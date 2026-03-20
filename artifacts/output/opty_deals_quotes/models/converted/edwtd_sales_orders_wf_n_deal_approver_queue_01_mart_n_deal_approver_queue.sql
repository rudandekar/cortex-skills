{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_approver_queue', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_APPROVER_QUEUE',
        'target_table': 'N_DEAL_APPROVER_QUEUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.977882+00:00'
    }
) }}

WITH 

source_w_deal_approver_queue AS (
    SELECT
        deal_approver_queue_key,
        sk_object_id_int,
        bk_deal_id,
        bk_quote_num,
        approval_scope_indicator_int,
        approval_queue_seq_int,
        approver_csco_wrkr_prty_key,
        approver_role_name,
        approver_team_id_int,
        approver_team_name,
        dat_member_csco_wrkr_prty_key,
        decision_indicator_int,
        decision_sla_hrs_cnt,
        is_current_approver_flg,
        decision_dtm,
        dv_decision_dt,
        decision_pending_from_dtm,
        dv_decision_pending_from_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        is_implicit_approver_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        reward_cd,
        approval_decision_status_desc,
        ss_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_approver_queue') }}
),

final AS (
    SELECT
        deal_approver_queue_key,
        sk_object_id_int,
        bk_deal_id,
        bk_quote_num,
        approval_scope_indicator_int,
        approval_queue_seq_int,
        approver_csco_wrkr_prty_key,
        approver_role_name,
        approver_team_id_int,
        approver_team_name,
        dat_member_csco_wrkr_prty_key,
        decision_indicator_int,
        decision_sla_hrs_cnt,
        is_current_approver_flg,
        decision_dtm,
        dv_decision_dt,
        decision_pending_from_dtm,
        dv_decision_pending_from_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        is_implicit_approver_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        reward_cd,
        approval_decision_status_desc,
        ss_code
    FROM source_w_deal_approver_queue
)

SELECT * FROM final