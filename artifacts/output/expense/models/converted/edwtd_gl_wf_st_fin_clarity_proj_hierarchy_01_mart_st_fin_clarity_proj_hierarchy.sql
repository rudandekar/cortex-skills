{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_clarity_proj_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_CLARITY_PROJ_HIERARCHY',
        'target_table': 'ST_FIN_CLARITY_PROJ_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.834541+00:00'
    }
) }}

WITH 

source_ff_fin_clarity_proj_hierarchy1 AS (
    SELECT
        clarity_project_id,
        clarity_project_name,
        clarity_project_desc
    FROM {{ source('raw', 'ff_fin_clarity_proj_hierarchy1') }}
),

final AS (
    SELECT
        clarity_project_id,
        clarity_project_name,
        clarity_project_desc
    FROM source_ff_fin_clarity_proj_hierarchy1
)

SELECT * FROM final