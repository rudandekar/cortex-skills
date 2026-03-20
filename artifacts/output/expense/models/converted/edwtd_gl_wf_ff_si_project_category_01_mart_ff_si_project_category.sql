{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_project_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_PROJECT_CATEGORY',
        'target_table': 'FF_SI_PROJECT_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.143638+00:00'
    }
) }}

WITH 

source_si_project_category AS (
    SELECT
        project_category_id,
        project_category_desc,
        enabled_flag,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        si_flex_struct_id
    FROM {{ source('raw', 'si_project_category') }}
),

transformed_exp_si_project_category AS (
    SELECT
    project_category_id,
    project_category_desc,
    enabled_flag,
    last_update_date,
    si_flex_struct_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_project_category
),

final AS (
    SELECT
        batch_id,
        project_category_id,
        project_category_desc,
        enabled_flag,
        si_flex_struct_id,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_project_category
)

SELECT * FROM final