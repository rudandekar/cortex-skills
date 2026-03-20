{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_mth_cntl_as_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_MTH_CNTL_AS_COGS',
        'target_table': 'WI_PNL_MTH_CNTL_AS_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.666037+00:00'
    }
) }}

WITH 

source_wi_pnl_mth_cntl_as_cogs AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_pnl_mth_cntl_as_cogs') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM source_wi_pnl_mth_cntl_as_cogs
)

SELECT * FROM final