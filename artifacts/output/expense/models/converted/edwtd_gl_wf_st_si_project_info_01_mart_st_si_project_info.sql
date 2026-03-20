{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_project_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_PROJECT_INFO',
        'target_table': 'ST_SI_PROJECT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.829511+00:00'
    }
) }}

WITH 

source_ff_si_project_info AS (
    SELECT
        batch_id,
        project_value,
        project_name,
        department_value,
        enabled_flag,
        si_flex_struct_id,
        parent_project_value,
        program_manager,
        project_category_id,
        reclass_project_value,
        spending_level,
        usage_description,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_project_info') }}
),

final AS (
    SELECT
        batch_id,
        project_value,
        project_name,
        department_value,
        enabled_flag,
        si_flex_struct_id,
        parent_project_value,
        program_manager,
        project_category_id,
        reclass_project_value,
        spending_level,
        usage_description,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_project_info
)

SELECT * FROM final