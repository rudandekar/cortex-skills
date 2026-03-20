{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_warranty_sr_case_entitlmnt', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_WARRANTY_SR_CASE_ENTITLMNT',
        'target_table': 'EL_WARRANTY_SR_CASE_ENTITLMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.645472+00:00'
    }
) }}

WITH 

source_st_warranty_sr_case_entitlmnt AS (
    SELECT
        sr_number,
        creation_date,
        fiscal_month_name,
        warranty_sr,
        rma_opened,
        fiscal_month_id
    FROM {{ source('raw', 'st_warranty_sr_case_entitlmnt') }}
),

final AS (
    SELECT
        sr_number,
        creation_date,
        fiscal_month_name,
        warranty_sr,
        rma_opened,
        fiscal_month_id
    FROM source_st_warranty_sr_case_entitlmnt
)

SELECT * FROM final