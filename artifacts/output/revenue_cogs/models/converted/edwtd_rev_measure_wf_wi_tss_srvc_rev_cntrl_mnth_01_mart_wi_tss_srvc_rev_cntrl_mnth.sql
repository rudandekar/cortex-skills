{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_srvc_rev_cntrl_mnth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_SRVC_REV_CNTRL_MNTH',
        'target_table': 'WI_TSS_SRVC_REV_CNTRL_MNTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.472428+00:00'
    }
) }}

WITH 

source_wi_tss_srvc_rev_cntrl_mnth AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_tss_srvc_rev_cntrl_mnth') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_tss_srvc_rev_cntrl_mnth
)

SELECT * FROM final