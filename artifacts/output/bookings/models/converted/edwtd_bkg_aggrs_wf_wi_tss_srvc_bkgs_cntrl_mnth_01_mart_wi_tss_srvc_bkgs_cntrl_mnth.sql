{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_srvc_bkgs_cntrl_mnth', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_SRVC_BKGS_CNTRL_MNTH',
        'target_table': 'WI_TSS_SRVC_BKGS_CNTRL_MNTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.310611+00:00'
    }
) }}

WITH 

source_wi_tss_srvc_bkgs_cntrl_mnth AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_tss_srvc_bkgs_cntrl_mnth') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_tss_srvc_bkgs_cntrl_mnth
)

SELECT * FROM final