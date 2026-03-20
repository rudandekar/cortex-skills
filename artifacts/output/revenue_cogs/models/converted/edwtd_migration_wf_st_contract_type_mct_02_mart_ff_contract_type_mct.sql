{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_contract_type_mct', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CONTRACT_TYPE_MCT',
        'target_table': 'FF_CONTRACT_TYPE_MCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.385306+00:00'
    }
) }}

WITH 

source_ff_contract_type_mct AS (
    SELECT
        coverage_template_name,
        pillar_desc,
        business_process_name
    FROM {{ source('raw', 'ff_contract_type_mct') }}
),

source_xxccs_ds_master_cvg_template AS (
    SELECT
        coverage_template_name,
        pillar_desc,
        business_process_name
    FROM {{ source('raw', 'xxccs_ds_master_cvg_template') }}
),

final AS (
    SELECT
        coverage_template_name,
        pillar_desc,
        business_process_name
    FROM source_xxccs_ds_master_cvg_template
)

SELECT * FROM final