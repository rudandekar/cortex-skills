{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_role_controls', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_ROLE_CONTROLS',
        'target_table': 'STG_CSF_PA_ROLE_CONTROLS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.638318+00:00'
    }
) }}

WITH 

source_csf_pa_role_controls AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        role_control_code,
        project_role_id,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_pa_role_controls') }}
),

source_stg_csf_pa_role_controls AS (
    SELECT
        role_control_code,
        project_role_id,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_role_controls') }}
),

transformed_exp_csf_pa_role_controls AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    role_control_code,
    project_role_id,
    record_version_number,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    zd_edition_name,
    zd_sync
    FROM source_stg_csf_pa_role_controls
),

final AS (
    SELECT
        role_control_code,
        project_role_id,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_role_controls
)

SELECT * FROM final