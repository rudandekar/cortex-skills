{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_mf_ap_notes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_MF_AP_NOTES',
        'target_table': 'EL_MF_AP_NOTES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.623736+00:00'
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

final AS (
    SELECT
        note_id,
        source_object_id,
        global_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        ges_update_date,
        notes_detail,
        notes_detail1,
        note_type,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_mf_ap_notes
)

SELECT * FROM final