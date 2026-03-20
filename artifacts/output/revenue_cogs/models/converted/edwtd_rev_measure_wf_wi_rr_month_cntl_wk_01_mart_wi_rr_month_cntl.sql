{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rr_month_cntl_wk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RR_MONTH_CNTL_WK',
        'target_table': 'WI_RR_MONTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.306466+00:00'
    }
) }}

WITH 

source_wi_rr_month_cntl AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM {{ source('raw', 'wi_rr_month_cntl') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM source_wi_rr_month_cntl
)

SELECT * FROM final