{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxcfi_gl_projects', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXCFI_GL_PROJECTS',
        'target_table': 'ST_CG1_XXCFI_GL_PROJECTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.651242+00:00'
    }
) }}

WITH 

source_st_cg1_xxcfi_gl_projects AS (
    SELECT
        batch_id,
        action_code,
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
        source_commit_time,
        refresh_datetime,
        global_name,
        create_datetime
    FROM {{ source('raw', 'st_cg1_xxcfi_gl_projects') }}
),

final AS (
    SELECT
        batch_id,
        action_code,
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
        source_commit_time,
        refresh_datetime,
        global_name,
        create_datetime
    FROM source_st_cg1_xxcfi_gl_projects
)

SELECT * FROM final