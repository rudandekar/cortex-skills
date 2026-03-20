{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_mth_cntl_srvc_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_MTH_CNTL_SRVC_REV',
        'target_table': 'WI_PNL_MTH_CNTL_SRVC_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.851045+00:00'
    }
) }}

WITH 

source_wi_pnl_mth_cntl_srvc_rev AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id
    FROM {{ source('raw', 'wi_pnl_mth_cntl_srvc_rev') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id
    FROM source_wi_pnl_mth_cntl_srvc_rev
)

SELECT * FROM final