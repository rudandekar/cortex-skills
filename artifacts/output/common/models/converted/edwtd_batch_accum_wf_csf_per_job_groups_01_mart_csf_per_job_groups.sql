{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_per_job_groups', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PER_JOB_GROUPS',
        'target_table': 'CSF_PER_JOB_GROUPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.119384+00:00'
    }
) }}

WITH 

source_csf_per_job_groups AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        job_group_id,
        business_group_id,
        legislation_code,
        internal_name,
        displayed_name,
        id_flex_num,
        master_flag,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'csf_per_job_groups') }}
),

source_stg_csf_per_job_groups AS (
    SELECT
        job_group_id,
        business_group_id,
        legislation_code,
        internal_name,
        displayed_name,
        id_flex_num,
        master_flag,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_per_job_groups') }}
),

transformed_exp_csf_per_job_groups AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    job_group_id,
    business_group_id,
    legislation_code,
    internal_name,
    displayed_name,
    id_flex_num,
    master_flag,
    object_version_number,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_stg_csf_per_job_groups
),

final AS (
    SELECT
        job_group_id,
        business_group_id,
        legislation_code,
        internal_name,
        displayed_name,
        id_flex_num,
        master_flag,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_per_job_groups
)

SELECT * FROM final