{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_mth_cntl_srvc_rev_rtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_MTH_CNTL_SRVC_REV_RTR',
        'target_table': 'WI_PNL_MTH_CNTL_SRVC_REV_RTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.128055+00:00'
    }
) }}

WITH 

source_wi_pnl_mth_cntl_srvc_rev_rtr AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM {{ source('raw', 'wi_pnl_mth_cntl_srvc_rev_rtr') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM source_wi_pnl_mth_cntl_srvc_rev_rtr
)

SELECT * FROM final