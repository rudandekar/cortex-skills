{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_vip_month_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_VIP_MONTH_CNTL',
        'target_table': 'WI_DEFREV_VIP_MONTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.094032+00:00'
    }
) }}

WITH 

source_wi_defrev_vip_month_cntl AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM {{ source('raw', 'wi_defrev_vip_month_cntl') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM source_wi_defrev_vip_month_cntl
)

SELECT * FROM final