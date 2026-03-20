{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rr_srvc_defrev_ccw_ar_wk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_RR_SRVC_DEFREV_CCW_AR_WK',
        'target_table': 'WI_GL_RR_SRVC_DEFREV_CCW_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.524138+00:00'
    }
) }}

WITH 

source_wi_gl_rr_srvc_defrev_ccw_ar AS (
    SELECT
        ar_trx_key
    FROM {{ source('raw', 'wi_gl_rr_srvc_defrev_ccw_ar') }}
),

final AS (
    SELECT
        ar_trx_key
    FROM source_wi_gl_rr_srvc_defrev_ccw_ar
)

SELECT * FROM final