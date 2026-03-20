{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_cmdm_adj_request', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_CMDM_ADJ_REQUEST',
        'target_table': 'N_AR_CMDM_ADJ_REQUEST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.605430+00:00'
    }
) }}

WITH 

source_w_ar_cmdm_adj_request AS (
    SELECT
        bk_saf_id_int,
        set_of_books_key,
        bk_company_cd,
        ss_cd,
        saf_type_cd,
        adjustment_reason_cd,
        adjustment_reason_txt,
        bk_requestor_party_key,
        bk_approver_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_cmdm_adj_request') }}
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
        saf_usd_amt
    FROM source_w_ar_cmdm_adj_request
)

SELECT * FROM final