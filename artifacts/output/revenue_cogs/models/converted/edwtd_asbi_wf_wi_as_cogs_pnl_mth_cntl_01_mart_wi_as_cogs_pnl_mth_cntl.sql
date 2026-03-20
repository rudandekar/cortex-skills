{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_pnl_mth_cntl', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PNL_MTH_CNTL',
        'target_table': 'WI_AS_COGS_PNL_MTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.273537+00:00'
    }
) }}

WITH 

source_wi_as_cogs_pnl_mth_cntl AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_as_cogs_pnl_mth_cntl') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM source_wi_as_cogs_pnl_mth_cntl
)

SELECT * FROM final