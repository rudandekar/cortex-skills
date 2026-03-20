{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxcfi_gl_projects', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXCFI_GL_PROJECTS',
        'target_table': 'CG1_XXCFI_GL_PROJECTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.689320+00:00'
    }
) }}

WITH 

source_cg1_xxcfi_gl_projects AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        project_id,
        project_segment,
        project_name,
        parent_program,
        business_unit_code,
        start_date_active,
        end_date_active,
        enabled_flag,
        project_mgr,
        department_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        last_updated_by,
        last_update_date,
        last_update_login,
        created_by,
        creation_date,
        summary_flag,
        ges_pk_id,
        project_name_20
    FROM {{ source('raw', 'cg1_xxcfi_gl_projects') }}
),

source_stg_cg1_xxcfi_gl_projects AS (
    SELECT
        project_id,
        project_segment,
        project_name,
        parent_program,
        business_unit_code,
        start_date_active,
        end_date_active,
        enabled_flag,
        project_mgr,
        department_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        last_updated_by,
        last_update_date,
        last_update_login,
        created_by,
        creation_date,
        summary_flag,
        ges_pk_id,
        project_name_20,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxcfi_gl_projects') }}
),

transformed_exp_cg1_xxcfi_gl_projects AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    project_id,
    project_segment,
    project_name,
    parent_program,
    business_unit_code,
    start_date_active,
    end_date_active,
    enabled_flag,
    project_mgr,
    department_id,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    last_updated_by,
    last_update_date,
    last_update_login,
    created_by,
    creation_date,
    summary_flag,
    ges_pk_id,
    project_name_20
    FROM source_stg_cg1_xxcfi_gl_projects
),

final AS (
    SELECT
        project_id,
        project_segment,
        project_name,
        parent_program,
        business_unit_code,
        start_date_active,
        end_date_active,
        enabled_flag,
        project_mgr,
        department_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        last_updated_by,
        last_update_date,
        last_update_login,
        created_by,
        creation_date,
        summary_flag,
        ges_pk_id,
        project_name_20,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxcfi_gl_projects
)

SELECT * FROM final