{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rec_rev_defrev_ccw_ar_wk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REC_REV_DEFREV_CCW_AR_WK',
        'target_table': 'WI_GL_REC_REV_DEFREV_CCW_AR_VIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.801029+00:00'
    }
) }}

WITH 

source_wi_gl_rec_rev_defrev_ccw_ar_vip AS (
    SELECT
        product_key,
        dv_attribution_cd,
        dv_product_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_gl_rec_rev_defrev_ccw_ar_vip') }}
),

source_wi_gl_rec_rev_defrev_ccw_ar AS (
    SELECT
        product_key,
        dv_attribution_cd,
        dv_product_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_gl_rec_rev_defrev_ccw_ar') }}
),

final AS (
    SELECT
        product_key,
        dv_attribution_cd,
        dv_product_key,
        dv_ar_trx_line_key
    FROM source_wi_gl_rec_rev_defrev_ccw_ar
)

SELECT * FROM final