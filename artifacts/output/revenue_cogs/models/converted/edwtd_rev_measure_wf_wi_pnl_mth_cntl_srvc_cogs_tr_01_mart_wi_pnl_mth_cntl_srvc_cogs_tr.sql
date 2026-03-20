{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_mth_cntl_srvc_cogs_tr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_MTH_CNTL_SRVC_COGS_TR',
        'target_table': 'WI_PNL_MTH_CNTL_SRVC_COGS_TR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.478200+00:00'
    }
) }}

WITH 

source_wi_pnl_mth_cntl_srvc_cogs_tr AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_pnl_mth_cntl_srvc_cogs_tr') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_pnl_mth_cntl_srvc_cogs_tr
)

SELECT * FROM final