{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rma_request', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_N_RMA_REQUEST',
        'target_table': 'N_RMA_REQUEST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.280895+00:00'
    }
) }}

WITH 

source_w_rma_request AS (
    SELECT
        bk_awaiting_authorization_num,
        rma_request_approval_reqd_role,
        rma_request_submit_dtm,
        dv_rma_request_submit_dt,
        rma_request_type_cd,
        rma_request_status_cd,
        rma_requestor_email_addr,
        rma_requestor_cco_id,
        original_sales_order_key,
        customer_return_cmt_txt,
        queue_admin_cmt_txt,
        bk_customer_return_reason_cd,
        approved_rma_sales_order_key,
        ru_apprvr_area_ctrlr_prty_key,
        ru_apprvr_rgnl_mgr_prty_key,
        ru_apprvr_director_prty_key,
        ru_rma_req_denial_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        last_modified_dtm,
        dv_last_modified_dt,
        restocking_fee_pct,
        rma_approved_dtm,
        dv_rma_approved_dt,
        crm_case_num,
        rqst_created_by_name,
        expedite_flg,
        hold_flg,
        xx_approver_id,
        xx_approved_by_proxy_id,
        action_code,
        dml_type,
        denial_reason_code,
        ru_apprvr_fin_vp_prty_key
    FROM {{ source('raw', 'w_rma_request') }}
),

final AS (
    SELECT
        bk_awaiting_authorization_num,
        rma_request_approval_reqd_role,
        rma_request_submit_dtm,
        dv_rma_request_submit_dt,
        rma_request_type_cd,
        rma_request_status_cd,
        rma_requestor_email_addr,
        rma_requestor_cco_id,
        original_sales_order_key,
        customer_return_cmt_txt,
        queue_admin_cmt_txt,
        bk_customer_return_reason_cd,
        approved_rma_sales_order_key,
        ru_apprvr_area_ctrlr_prty_key,
        ru_apprvr_rgnl_mgr_prty_key,
        ru_apprvr_director_prty_key,
        ru_rma_req_denial_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        last_modified_dtm,
        dv_last_modified_dt,
        restocking_fee_pct,
        rma_approved_dtm,
        dv_rma_approved_dt,
        crm_case_num,
        rqst_created_by_name,
        expedite_flg,
        hold_flg,
        xx_approver_id,
        xx_approved_by_proxy_id,
        denial_reason_code,
        ru_apprvr_fin_vp_prty_key
    FROM source_w_rma_request
)

SELECT * FROM final