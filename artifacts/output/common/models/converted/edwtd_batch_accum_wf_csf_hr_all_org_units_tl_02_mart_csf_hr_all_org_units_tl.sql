{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_hr_all_org_units_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_HR_ALL_ORG_UNITS_TL',
        'target_table': 'CSF_HR_ALL_ORG_UNITS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.573076+00:00'
    }
) }}

WITH 

source_csf_hr_all_org_units_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        organization_id,
        language,
        source_lang,
        name,
        last_update_date,
        last_updated_by,
        last_update_login,
        created_by,
        creation_date,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_hr_all_org_units_tl') }}
),

source_stg_csf_hr_all_org_units_tl AS (
    SELECT
        organization_id,
        language_,
        source_lang,
        name,
        last_update_date,
        last_updated_by,
        last_update_login,
        created_by,
        creation_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_hr_all_org_units_tl') }}
),

transformed_exp_csf_hr_all_org_units_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    organization_id,
    language,
    source_lang,
    name,
    last_update_date,
    last_updated_by,
    last_update_login,
    created_by,
    creation_date,
    zd_edition_name,
    zd_sync
    FROM source_stg_csf_hr_all_org_units_tl
),

final AS (
    SELECT
        organization_id,
        language_,
        source_lang,
        name,
        last_update_date,
        last_updated_by,
        last_update_login,
        created_by,
        creation_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_hr_all_org_units_tl
)

SELECT * FROM final