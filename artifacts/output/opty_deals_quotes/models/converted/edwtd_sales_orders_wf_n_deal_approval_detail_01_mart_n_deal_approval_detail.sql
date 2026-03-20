{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_approval_detail', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_APPROVAL_DETAIL',
        'target_table': 'N_DEAL_APPROVAL_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.879386+00:00'
    }
) }}

WITH 

source_w_deal_approval_detail AS (
    SELECT
        deal_approval_key,
        bk_deal_id,
        approval_effective_from_dtm,
        deal_queue_status_cd,
        dv_approval_effective_from_dt,
        approval_effective_to_dtm,
        dv_approval_effective_to_dt,
        bk_deal_decision_name,
        bk_approver_role_name,
        queue_sequence_int,
        src_rptd_submitter_id,
        dv_apprvr_csco_wrkr_pty_key,
        src_rptd_approver_id,
        dv_submitter_csco_wrkr_pty_key,
        approval_source_cd,
        decision_on_dtm,
        dv_decision_on_dt,
        approval_category_cd,
        user_role_cd,
        approver_comments_txt,
        sk_object_id_int,
        approval_deal_type_cd,
        bk_approver_group_type_name,
        submitted_on_dtm,
        dv_submitted_on_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_approval_detail') }}
),

final AS (
    SELECT
        deal_approval_key,
        bk_deal_id,
        approval_effective_from_dtm,
        deal_queue_status_cd,
        dv_approval_effective_from_dt,
        approval_effective_to_dtm,
        dv_approval_effective_to_dt,
        bk_deal_decision_name,
        bk_approver_role_name,
        queue_sequence_int,
        src_rptd_submitter_id,
        dv_apprvr_csco_wrkr_pty_key,
        src_rptd_approver_id,
        dv_submitter_csco_wrkr_pty_key,
        approval_source_cd,
        decision_on_dtm,
        dv_decision_on_dt,
        approval_category_cd,
        user_role_cd,
        approver_comments_txt,
        sk_object_id_int,
        approval_deal_type_cd,
        bk_approver_group_type_name,
        submitted_on_dtm,
        dv_submitted_on_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal_approval_detail
)

SELECT * FROM final