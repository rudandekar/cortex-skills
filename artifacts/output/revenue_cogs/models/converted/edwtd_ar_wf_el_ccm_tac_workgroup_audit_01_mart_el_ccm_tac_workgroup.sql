{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccm_tac_workgroup', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CCM_TAC_WORKGROUP',
        'target_table': 'EL_CCM_TAC_WORKGROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.552662+00:00'
    }
) }}

WITH 

source_st_ccm_tac_workgroup AS (
    SELECT
        fiscal_year_month_int,
        wg_name,
        theater,
        delivery_channel,
        ccm_theater,
        override_theater
    FROM {{ source('raw', 'st_ccm_tac_workgroup') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        wg_name,
        theater,
        delivery_channel,
        ccm_theater,
        override_theater
    FROM source_st_ccm_tac_workgroup
)

SELECT * FROM final