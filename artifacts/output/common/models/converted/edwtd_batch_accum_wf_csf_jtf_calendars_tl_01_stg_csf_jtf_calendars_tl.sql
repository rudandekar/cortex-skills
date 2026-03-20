{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_jtf_calendars_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_JTF_CALENDARS_TL',
        'target_table': 'STG_CSF_JTF_CALENDARS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.928823+00:00'
    }
) }}

WITH 

source_stg_csf_jtf_calendars_tl AS (
    SELECT
        calendar_id,
        calendar_name,
        description,
        language_,
        source_lang,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_jtf_calendars_tl') }}
),

source_csf_jtf_calendars_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        calendar_id,
        calendar_name,
        description,
        language,
        source_lang,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id
    FROM {{ source('raw', 'csf_jtf_calendars_tl') }}
),

transformed_exp_csf_jtf_calendars_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    calendar_id,
    calendar_name,
    description,
    language,
    source_lang,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id
    FROM source_csf_jtf_calendars_tl
),

final AS (
    SELECT
        calendar_id,
        calendar_name,
        description,
        language_,
        source_lang,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_jtf_calendars_tl
)

SELECT * FROM final