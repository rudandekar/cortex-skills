{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_srvc_unbill_ctrl_mth_acq', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AS_SRVC_UNBILL_CTRL_MTH_ACQ',
        'target_table': 'WI_AS_SRVC_UNBILL_CTRL_MTH_ACQ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.785042+00:00'
    }
) }}

WITH 

source_wi_as_srvc_unbill_ctrl_mth_acq AS (
    SELECT
        transaction_type,
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_as_srvc_unbill_ctrl_mth_acq') }}
),

final AS (
    SELECT
        transaction_type,
        fiscal_year_month_int
    FROM source_wi_as_srvc_unbill_ctrl_mth_acq
)

SELECT * FROM final