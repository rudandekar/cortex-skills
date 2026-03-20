{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxccs_ds_sahdr_core_rma', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_XXCCS_DS_SAHDR_CORE_RMA',
        'target_table': 'ST_XXCCS_DS_SAHDR_CORE_RMA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.116899+00:00'
    }
) }}

WITH 

source_ff_xxccs_ds_sahdr_core_rma AS (
    SELECT
        contract_number,
        service_line_id,
        bill_to_site_use_id,
        billto_gu_id,
        services_full_coverage,
        cxea_flag,
        service_line_name
    FROM {{ source('raw', 'ff_xxccs_ds_sahdr_core_rma') }}
),

final AS (
    SELECT
        contract_number,
        service_line_id,
        bill_to_site_use_id,
        billto_gu_id,
        services_full_coverage,
        cxea_flag,
        service_line_name
    FROM source_ff_xxccs_ds_sahdr_core_rma
)

SELECT * FROM final