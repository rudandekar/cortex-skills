{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccm_tac_sr_acod', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_CCM_TAC_SR_ACOD',
        'target_table': 'EL_CCM_TAC_SR_ACOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.441740+00:00'
    }
) }}

WITH 

source_el_ccm_tac_sr_acod AS (
    SELECT
        fiscal_year_month_int,
        sr_number,
        wg_name,
        accrued_acod,
        fin_acod
    FROM {{ source('raw', 'el_ccm_tac_sr_acod') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        sr_number,
        wg_name,
        accrued_acod,
        fin_acod
    FROM source_el_ccm_tac_sr_acod
)

SELECT * FROM final