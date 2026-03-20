{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_expense_auditor_notes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_EXPENSE_AUDITOR_NOTES',
        'target_table': 'W_EXPENSE_AUDITOR_NOTES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.900635+00:00'
    }
) }}

WITH 

source_st_mf_ap_notes AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        note_id,
        source_object_code,
        source_object_id,
        note_type,
        entered_by,
        entered_date,
        source_lang,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        notes_detail,
        last_update_login,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'st_mf_ap_notes') }}
),

source_sm_expense_report AS (
    SELECT
        expense_report_key,
        bk_ss_cd,
        edw_create_user,
        edw_create_dtm,
        sk_report_header_id_int
    FROM {{ source('raw', 'sm_expense_report') }}
),

source_st_mf_ap_notes AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        note_id,
        source_object_code,
        source_object_id,
        note_type,
        entered_by,
        entered_date,
        source_lang,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        notes_detail,
        last_update_login,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'st_mf_ap_notes') }}
),

source_sm_expense_report AS (
    SELECT
        expense_report_key,
        bk_ss_cd,
        edw_create_user,
        edw_create_dtm,
        sk_report_header_id_int
    FROM {{ source('raw', 'sm_expense_report') }}
),

source_ex_mf_ap_notes AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        note_id,
        source_object_code,
        source_object_id,
        note_type,
        entered_by,
        entered_date,
        source_lang,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        notes_detail,
        last_update_login,
        ges_update_date,
        global_name,
        exception_type
    FROM {{ source('raw', 'ex_mf_ap_notes') }}
),

transformed_exptrans AS (
    SELECT
    batch_id,
    create_datetime,
    action_code,
    note_id,
    source_object_code,
    source_object_id,
    note_type,
    entered_by,
    entered_date,
    source_lang,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    notes_detail,
    last_update_login,
    ges_update_date,
    global_name,
    'RI' AS exception_type
    FROM source_ex_mf_ap_notes
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
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final