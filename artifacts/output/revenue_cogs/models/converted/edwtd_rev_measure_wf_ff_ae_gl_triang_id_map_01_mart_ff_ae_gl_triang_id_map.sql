{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ae_gl_triang_id_map', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_AE_GL_TRIANG_ID_MAP',
        'target_table': 'FF_AE_GL_TRIANG_ID_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.782052+00:00'
    }
) }}

WITH 

source_ae_gl_triang_id_map AS (
    SELECT
        triangulation_id,
        allocation_type_name,
        active_flag
    FROM {{ source('raw', 'ae_gl_triang_id_map') }}
),

final AS (
    SELECT
        triangulation_id,
        allocation_type_name,
        active_flag
    FROM source_ae_gl_triang_id_map
)

SELECT * FROM final