{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccm_tac_workgroup', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CCM_TAC_WORKGROUP',
        'target_table': 'ST_CCM_TAC_WORKGROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.834536+00:00'
    }
) }}

WITH 

source_ff_ccm_tac_workgroup AS (
    SELECT
        fiscal_month_id,
        wg_name,
        theater,
        delivery_channel,
        ccm_theater,
        override_theater
    FROM {{ source('raw', 'ff_ccm_tac_workgroup') }}
),

final AS (
    SELECT
        fiscal_month_id,
        wg_name,
        theater,
        delivery_channel,
        ccm_theater,
        override_theater
    FROM source_ff_ccm_tac_workgroup
)

SELECT * FROM final