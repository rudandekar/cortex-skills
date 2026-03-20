{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_qtr_act_mnth_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_QTR_ACT_MNTH_CNTL',
        'target_table': 'WI_QTR_ACT_MNTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.991773+00:00'
    }
) }}

WITH 

source_wi_qtr_act_mnth_cntl AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_qtr_act_mnth_cntl') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_qtr_act_mnth_cntl
)

SELECT * FROM final