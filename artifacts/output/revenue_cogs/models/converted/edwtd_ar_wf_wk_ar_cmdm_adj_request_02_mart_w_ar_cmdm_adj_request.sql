{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_cmdm_adj_request', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_AR_CMDM_ADJ_REQUEST',
        'target_table': 'W_AR_CMDM_ADJ_REQUEST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.604763+00:00'
    }
) }}

WITH 

source_st_cg_xxicm_saf_headers_rev AS (
    SELECT
        batch_id,
        saf_header_id,
        saf_id,
        saf_type,
        saf_reason,
        saf_date,
        saf_status,
        saf_amount,
        special_request,
        email_address,
        initiator_role_id,
        initiator_group_id,
        collector_id,
        comments,
        item_key,
        cash_receipt_id,
        alert_status,
        org_id,
        func_saf_amount,
        func_currency_code,
        tran_currency_code,
        exchange_rate_type,
        exchange_rate,
        exchange_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_commit_time,
        global_name,
        create_datetime,
        action_code,
        d_attribute1,
        n_attribute1,
        d_attribute2
    FROM {{ source('raw', 'st_cg_xxicm_saf_headers_rev') }}
),

source_st_om_xxcac_saf_hdr_all_rev AS (
    SELECT
        batch_id,
        alert_status,
        cash_receipt_id,
        collector_id,
        comments,
        created_by,
        creation_date,
        email_address,
        exchange_date,
        exchange_rate,
        exchange_rate_type,
        func_currency_code,
        func_saf_amount,
        ges_update_date,
        global_name,
        initiator_group_id,
        initiator_role_id,
        item_key,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        saf_amount,
        saf_date,
        saf_header_id,
        saf_id,
        saf_reason,
        saf_status,
        saf_type,
        special_request,
        tran_currency_code,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxcac_saf_hdr_all_rev') }}
),

final AS (
    SELECT
        bk_saf_id_int,
        set_of_books_key,
        bk_company_cd,
        ss_cd,
        saf_type_cd,
        adjustment_reason_cd,
        adjustment_reason_txt,
        bk_approver_party_key,
        bk_requestor_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        pending_approver_cnt,
        estimated_saf_approval_dt,
        ar_trx_line_key,
        source_created_dtm,
        dv_source_created_dt,
        bk_sales_adj_form_status_cd,
        saf_usd_amt,
        action_code,
        dml_type
    FROM source_st_om_xxcac_saf_hdr_all_rev
)

SELECT * FROM final