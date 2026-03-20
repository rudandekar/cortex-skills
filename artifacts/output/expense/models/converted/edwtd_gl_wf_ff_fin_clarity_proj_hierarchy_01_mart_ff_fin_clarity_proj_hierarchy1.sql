{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_fin_clarity_proj_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_FIN_CLARITY_PROJ_HIERARCHY',
        'target_table': 'FF_FIN_CLARITY_PROJ_HIERARCHY1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.661263+00:00'
    }
) }}

WITH 

source_fin_clarity_proj_hierarchy AS (
    SELECT
        clarity_project_id,
        clarity_project_name,
        clarity_project_desc
    FROM {{ source('raw', 'fin_clarity_proj_hierarchy') }}
),

final AS (
    SELECT
        clarity_project_id,
        clarity_project_name,
        clarity_project_desc
    FROM source_fin_clarity_proj_hierarchy
)

SELECT * FROM final