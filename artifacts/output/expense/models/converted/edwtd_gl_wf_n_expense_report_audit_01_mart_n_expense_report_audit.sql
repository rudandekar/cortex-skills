{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_report_audit', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_REPORT_AUDIT',
        'target_table': 'N_EXPENSE_REPORT_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.892667+00:00'
    }
) }}

WITH 

source_w_expense_report_audit AS (
    SELECT
        expense_report_audit_key,
        sk_audit_reason_id_int,
        expense_report_key,
        audit_reason_cd,
        src_created_dtm,
        dv_src_created_dt,
        src_crtd_csco_wrkr_prty_key,
        src_updated_dtm,
        dv_src_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_report_audit') }}
),

final AS (
    SELECT
        expense_report_audit_key,
        sk_audit_reason_id_int,
        expense_report_key,
        audit_reason_cd,
        src_created_dtm,
        dv_src_created_dt,
        src_crtd_csco_wrkr_prty_key,
        src_updated_dtm,
        dv_src_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_expense_report_audit
)

SELECT * FROM final