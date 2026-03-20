{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_auditor_notes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_AUDITOR_NOTES',
        'target_table': 'N_EXPENSE_AUDITOR_NOTES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.020644+00:00'
    }
) }}

WITH 

source_w_expense_auditor_notes AS (
    SELECT
        expense_auditor_notes_key,
        expense_report_key,
        er_detailed_auditor_notes_txt,
        dv_er_detl_aud_abbr_notes_txt,
        source_deleted_flg,
        entered_by_csco_wrkr_prty_key,
        entered_dtm,
        dv_entered_dt,
        sk_note_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_auditor_notes') }}
),

final AS (
    SELECT
        expense_auditor_notes_key,
        expense_report_key,
        er_detailed_auditor_notes_txt,
        dv_er_detl_aud_abbr_notes_txt,
        source_deleted_flg,
        entered_by_csco_wrkr_prty_key,
        entered_dtm,
        dv_entered_dt,
        sk_note_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_expense_auditor_notes
)

SELECT * FROM final