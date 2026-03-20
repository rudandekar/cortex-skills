{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_chnl_rev_month_cntl_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_CHNL_REV_MONTH_CNTL_MTH',
        'target_table': 'WI_GL_CHNL_REV_MONTH_CNTL_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.197519+00:00'
    }
) }}

WITH 

source_wi_gl_chnl_rev_month_cntl_mth AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_gl_chnl_rev_month_cntl_mth') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_gl_chnl_rev_month_cntl_mth
)

SELECT * FROM final