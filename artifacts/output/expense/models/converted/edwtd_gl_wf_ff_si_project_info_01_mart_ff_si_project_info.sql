{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_project_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_PROJECT_INFO',
        'target_table': 'FF_SI_PROJECT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.832990+00:00'
    }
) }}

WITH 

source_si_project_info AS (
    SELECT
        project_value,
        si_flex_struct_id,
        business_unit_id,
        project_category_id,
        program_manager,
        project_name,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        start_date,
        end_date,
        enabled_flag,
        is_parent_flag,
        parent_project_value,
        spending_level,
        usage_description,
        info_published,
        erp_published,
        reclass_project_value,
        department_value
    FROM {{ source('raw', 'si_project_info') }}
),

transformed_exp_si_project_info AS (
    SELECT
    project_value,
    si_flex_struct_id,
    parent_project_value,
    program_manager,
    project_category_id,
    reclass_project_value,
    spending_level,
    usage_description,
    last_update_date,
    project_name,
    enabled_flag,
    department_value,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_project_info
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
    FROM transformed_exp_si_project_info
)

SELECT * FROM final