{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_rol_trx_line_gl_distri', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_SM_ROL_TRX_LINE_GL_DISTRI',
        'target_table': 'SM_ROL_TRX_LINE_GL_DISTRI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.066392+00:00'
    }
) }}

WITH 

source_sm_rol_trx_line_gl_distri AS (
    SELECT
        rol_trx_line_gl_distri_key,
        sk_trx_distri_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_rol_trx_line_gl_distri') }}
),

final AS (
    SELECT
        rol_trx_line_gl_distri_key,
        sk_trx_distri_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_rol_trx_line_gl_distri
)

SELECT * FROM final