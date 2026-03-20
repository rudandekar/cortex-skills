{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_ofrvw_cfa_mth_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_OFRVW_CFA_MTH_CNTL',
        'target_table': 'WI_DEFREV_OFRVW_CFA_MTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.719180+00:00'
    }
) }}

WITH 

source_wi_defrev_ofrvw_cfa_mth_cntl AS (
    SELECT
        fiscal_year_month_int
    FROM {{ source('raw', 'wi_defrev_ofrvw_cfa_mth_cntl') }}
),

final AS (
    SELECT
        fiscal_year_month_int
    FROM source_wi_defrev_ofrvw_cfa_mth_cntl
)

SELECT * FROM final