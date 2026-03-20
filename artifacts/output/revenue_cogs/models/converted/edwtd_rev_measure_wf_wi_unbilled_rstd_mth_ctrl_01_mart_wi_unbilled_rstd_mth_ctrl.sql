{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbilled_rstd_mth_ctrl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_UNBILLED_RSTD_MTH_CTRL',
        'target_table': 'WI_UNBILLED_RSTD_MTH_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.212559+00:00'
    }
) }}

WITH 

source_wi_unbilled_rstd_mth_ctrl AS (
    SELECT
        transaction_type,
        fiscal_year_mth_number_int
    FROM {{ source('raw', 'wi_unbilled_rstd_mth_ctrl') }}
),

final AS (
    SELECT
        transaction_type,
        fiscal_year_mth_number_int
    FROM source_wi_unbilled_rstd_mth_ctrl
)

SELECT * FROM final