{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_mth_cntl_tss_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_MTH_CNTL_TSS_COGS',
        'target_table': 'WI_PNL_MTH_CNTL_TSS_COGS_RTM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.917241+00:00'
    }
) }}

WITH 

source_wi_pnl_mth_cntl_tss_cogs_rtm AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM {{ source('raw', 'wi_pnl_mth_cntl_tss_cogs_rtm') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM source_wi_pnl_mth_cntl_tss_cogs_rtm
)

SELECT * FROM final