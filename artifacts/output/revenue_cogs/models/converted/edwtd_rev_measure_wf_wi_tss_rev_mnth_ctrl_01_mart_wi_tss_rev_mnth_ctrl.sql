{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_rev_mnth_ctrl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_REV_MNTH_CTRL',
        'target_table': 'WI_TSS_REV_MNTH_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.196215+00:00'
    }
) }}

WITH 

source_wi_tss_rev_mnth_ctrl AS (
    SELECT
        rolling_fiscal_yr_mth_num_int,
        fiscal_year_mth_number_int
    FROM {{ source('raw', 'wi_tss_rev_mnth_ctrl') }}
),

final AS (
    SELECT
        rolling_fiscal_yr_mth_num_int,
        fiscal_year_mth_number_int
    FROM source_wi_tss_rev_mnth_ctrl
)

SELECT * FROM final