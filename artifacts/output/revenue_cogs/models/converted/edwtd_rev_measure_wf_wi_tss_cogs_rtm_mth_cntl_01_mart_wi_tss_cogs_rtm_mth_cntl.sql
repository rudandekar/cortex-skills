{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_cogs_rtm_mth_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_COGS_RTM_MTH_CNTL',
        'target_table': 'WI_TSS_COGS_RTM_MTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.156902+00:00'
    }
) }}

WITH 

source_wi_tss_cogs_rtm_mth_cntl AS (
    SELECT
        current_fiscal_year_month_int,
        target_fiscal_year_month_int,
        driver_fiscal_year_month_int
    FROM {{ source('raw', 'wi_tss_cogs_rtm_mth_cntl') }}
),

final AS (
    SELECT
        current_fiscal_year_month_int,
        target_fiscal_year_month_int,
        driver_fiscal_year_month_int
    FROM source_wi_tss_cogs_rtm_mth_cntl
)

SELECT * FROM final