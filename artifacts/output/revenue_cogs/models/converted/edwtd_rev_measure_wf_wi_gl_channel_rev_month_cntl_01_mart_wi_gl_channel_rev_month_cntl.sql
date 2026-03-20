{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_channel_rev_month_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_CHANNEL_REV_MONTH_CNTL',
        'target_table': 'WI_GL_CHANNEL_REV_MONTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.719359+00:00'
    }
) }}

WITH 

source_wi_gl_channel_rev_month_cntl AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_gl_channel_rev_month_cntl') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_gl_channel_rev_month_cntl
)

SELECT * FROM final