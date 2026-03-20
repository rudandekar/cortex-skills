{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_srvc_unbill_ctrl_mth_adj', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_SRVC_UNBILL_CTRL_MTH_ADJ',
        'target_table': 'WI_TSS_SRVC_UNBILL_CTRL_MTH_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.720768+00:00'
    }
) }}

WITH 

source_wi_tss_srvc_unbill_ctrl_mth_adj AS (
    SELECT
        transaction_type,
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_tss_srvc_unbill_ctrl_mth_adj') }}
),

final AS (
    SELECT
        transaction_type,
        fiscal_year_month_int
    FROM source_wi_tss_srvc_unbill_ctrl_mth_adj
)

SELECT * FROM final