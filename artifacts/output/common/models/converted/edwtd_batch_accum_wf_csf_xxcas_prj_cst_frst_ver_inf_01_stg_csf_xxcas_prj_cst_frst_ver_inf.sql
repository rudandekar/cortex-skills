{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_cst_frst_ver_inf', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_CST_FRST_VER_INF',
        'target_table': 'STG_CSF_XXCAS_PRJ_CST_FRST_VER_INF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.103800+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_cst_frst_ver_inf AS (
    SELECT
        forecast_version_id,
        version_number,
        version_name,
        project_id,
        baselined_by_person_id,
        baselined_date,
        version_status,
        current_flag,
        copied_from_version_id,
        mig_to_oob_status,
        last_submitted_date,
        last_updated_by,
        object_version_number,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_submitted_by,
        change_reason,
        comments,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_cst_frst_ver_inf') }}
),

source_csf_xxcas_prj_cst_frst_vr_info AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        forecast_version_id,
        version_number,
        version_name,
        project_id,
        baselined_by_person_id,
        baselined_date,
        version_status,
        current_flag,
        copied_from_version_id,
        mig_to_oob_status,
        last_submitted_date,
        last_updated_by,
        object_version_number,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_submitted_by,
        change_reason,
        comments
    FROM {{ source('raw', 'csf_xxcas_prj_cst_frst_vr_info') }}
),

transformed_exp_csf_xxcas_prj_cst_frst_vr_info AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    forecast_version_id,
    version_number,
    version_name,
    project_id,
    baselined_by_person_id,
    baselined_date,
    version_status,
    current_flag,
    copied_from_version_id,
    mig_to_oob_status,
    last_submitted_date,
    last_updated_by,
    object_version_number,
    last_update_login,
    created_by,
    creation_date,
    last_update_date,
    last_submitted_by,
    change_reason,
    comments
    FROM source_csf_xxcas_prj_cst_frst_vr_info
),

final AS (
    SELECT
        forecast_version_id,
        version_number,
        version_name,
        project_id,
        baselined_by_person_id,
        baselined_date,
        version_status,
        current_flag,
        copied_from_version_id,
        mig_to_oob_status,
        last_submitted_date,
        last_updated_by,
        object_version_number,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_submitted_by,
        change_reason,
        comments,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_cst_frst_vr_info
)

SELECT * FROM final