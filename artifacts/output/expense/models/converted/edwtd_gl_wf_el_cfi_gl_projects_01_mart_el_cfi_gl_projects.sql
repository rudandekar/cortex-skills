{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cfi_gl_projects', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_CFI_GL_PROJECTS',
        'target_table': 'EL_CFI_GL_PROJECTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.594529+00:00'
    }
) }}

WITH 

source_st_mf_cfi_gl_projects AS (
    SELECT
        batch_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        business_unit_code,
        created_by,
        creation_date,
        department_id,
        enabled_flag,
        end_date_active,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        parent_program,
        project_id,
        project_mgr,
        project_name,
        project_segment,
        start_date_active,
        summary_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_cfi_gl_projects') }}
),

final AS (
    SELECT
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        business_unit_code,
        created_by,
        creation_date,
        department_id,
        enabled_flag,
        end_date_active,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        parent_program,
        project_id,
        project_mgr,
        project_name,
        project_segment,
        start_date_active,
        summary_flag
    FROM source_st_mf_cfi_gl_projects
)

SELECT * FROM final