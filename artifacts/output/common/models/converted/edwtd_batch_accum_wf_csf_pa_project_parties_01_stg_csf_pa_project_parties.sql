{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_project_parties', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_PROJECT_PARTIES',
        'target_table': 'STG_CSF_PA_PROJECT_PARTIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.635984+00:00'
    }
) }}

WITH 

source_csf_pa_project_parties AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        project_party_id,
        object_id,
        object_type,
        project_id,
        resource_id,
        resource_type_id,
        resource_source_id,
        project_role_id,
        start_date_active,
        end_date_active,
        scheduled_flag,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        grant_id
    FROM {{ source('raw', 'csf_pa_project_parties') }}
),

source_stg_csf_pa_project_parties AS (
    SELECT
        project_party_id,
        object_id,
        object_type,
        project_id,
        resource_id,
        resource_type_id,
        resource_source_id,
        project_role_id,
        start_date_active,
        end_date_active,
        scheduled_flag,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        grant_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_project_parties') }}
),

transformed_exp_csf_pa_project_parties AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    project_party_id,
    object_id,
    object_type,
    project_id,
    resource_id,
    resource_type_id,
    resource_source_id,
    project_role_id,
    start_date_active,
    end_date_active,
    scheduled_flag,
    record_version_number,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    grant_id
    FROM source_stg_csf_pa_project_parties
),

final AS (
    SELECT
        project_party_id,
        object_id,
        object_type,
        project_id,
        resource_id,
        resource_type_id,
        resource_source_id,
        project_role_id,
        start_date_active,
        end_date_active,
        scheduled_flag,
        record_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        grant_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_project_parties
)

SELECT * FROM final