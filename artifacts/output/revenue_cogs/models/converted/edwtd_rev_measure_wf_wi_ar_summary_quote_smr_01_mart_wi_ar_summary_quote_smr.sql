{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ar_summary_quote_smr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_AR_SUMMARY_QUOTE_SMR',
        'target_table': 'WI_AR_SUMMARY_QUOTE_SMR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.991421+00:00'
    }
) }}

WITH 

source_wi_ar_summary_quote_smr AS (
    SELECT
        sales_order_line_key,
        dv_effective_dtm,
        sales_motion_cd,
        edw_create_user,
        edw_create_dtm,
        dv_allocation_pct
    FROM {{ source('raw', 'wi_ar_summary_quote_smr') }}
),

final AS (
    SELECT
        sales_order_line_key,
        dv_effective_dtm,
        sales_motion_cd,
        edw_create_user,
        edw_create_dtm,
        dv_allocation_pct
    FROM source_wi_ar_summary_quote_smr
)

SELECT * FROM final