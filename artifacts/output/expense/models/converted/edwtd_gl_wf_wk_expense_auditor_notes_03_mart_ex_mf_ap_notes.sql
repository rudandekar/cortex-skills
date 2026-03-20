{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_expense_auditor_notes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_EXPENSE_AUDITOR_NOTES',
        'target_table': 'EX_MF_AP_NOTES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.833373+00:00'
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
    FROM transformed_exptrans
)

SELECT * FROM final