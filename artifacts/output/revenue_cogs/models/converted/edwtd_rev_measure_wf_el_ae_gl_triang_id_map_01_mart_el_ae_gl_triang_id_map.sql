{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ae_gl_triang_id_map', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_AE_GL_TRIANG_ID_MAP',
        'target_table': 'EL_AE_GL_TRIANG_ID_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.530531+00:00'
    }
) }}

WITH 

source_st_ae_gl_triang_id_map AS (
    SELECT
        triangulation_id,
        allocation_type_name,
        active_flag
    FROM {{ source('raw', 'st_ae_gl_triang_id_map') }}
),

source_el_ae_gl_triang_id_map AS (
    SELECT
        triangulation_id,
        allocation_type_name,
        active_flag,
        create_user,
        create_datetime
    FROM {{ source('raw', 'el_ae_gl_triang_id_map') }}
),

final AS (
    SELECT
        triangulation_id,
        allocation_type_name,
        active_flag,
        create_user,
        create_datetime
    FROM source_el_ae_gl_triang_id_map
)

SELECT * FROM final