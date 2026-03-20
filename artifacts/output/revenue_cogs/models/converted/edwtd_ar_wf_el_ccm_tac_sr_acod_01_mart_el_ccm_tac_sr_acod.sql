{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccm_tac_sr_acod', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CCM_TAC_SR_ACOD',
        'target_table': 'EL_CCM_TAC_SR_ACOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.067977+00:00'
    }
) }}

WITH 

source_st_tac_twfm025_sr_acod1 AS (
    SELECT
        fiscal_month_id,
        sr_number,
        wg_name,
        theater,
        delivery_channel,
        accrued_acod
    FROM {{ source('raw', 'st_tac_twfm025_sr_acod1') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        sr_number,
        wg_name,
        accrued_acod
    FROM source_st_tac_twfm025_sr_acod1
)

SELECT * FROM final