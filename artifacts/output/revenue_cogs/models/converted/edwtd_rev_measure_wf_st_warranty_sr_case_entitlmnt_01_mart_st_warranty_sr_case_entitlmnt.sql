{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_warranty_sr_case_entitlmnt', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_WARRANTY_SR_CASE_ENTITLMNT',
        'target_table': 'ST_WARRANTY_SR_CASE_ENTITLMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.108914+00:00'
    }
) }}

WITH 

source_ff_warranty_sr_case_entitlmnt AS (
    SELECT
        sr_number,
        creation_date,
        fiscal_month_name,
        warranty_sr,
        rma_order,
        fisical_month_id
    FROM {{ source('raw', 'ff_warranty_sr_case_entitlmnt') }}
),

final AS (
    SELECT
        sr_number,
        creation_date,
        fiscal_month_name,
        warranty_sr,
        rma_opened,
        fiscal_month_id
    FROM source_ff_warranty_sr_case_entitlmnt
)

SELECT * FROM final