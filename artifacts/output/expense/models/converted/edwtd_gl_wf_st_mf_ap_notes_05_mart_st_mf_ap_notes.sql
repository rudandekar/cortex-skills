{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mf_ap_notes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MF_AP_NOTES',
        'target_table': 'ST_MF_AP_NOTES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.686074+00:00'
    }
) }}

WITH 

source_mf_ap_notes AS (
    SELECT
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
    FROM {{ source('raw', 'mf_ap_notes') }}
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

transformed_exptrans AS (
    SELECT
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
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_st_mf_ap_notes
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
        global_name
    FROM transformed_exptrans
)

SELECT * FROM final