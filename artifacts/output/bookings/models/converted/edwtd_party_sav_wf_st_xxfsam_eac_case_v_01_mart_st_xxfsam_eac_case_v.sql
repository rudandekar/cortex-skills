{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_case_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_CASE_V',
        'target_table': 'ST_XXFSAM_EAC_CASE_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.729507+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_case_v AS (
    SELECT
        case_id,
        case_type,
        module,
        source,
        status,
        auto_approved,
        effective_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        approver,
        reason_code,
        reason_description,
        organization_name,
        organization_id
    FROM {{ source('raw', 'ff_xxfsam_eac_case_v') }}
),

final AS (
    SELECT
        case_id,
        case_type,
        module,
        source,
        status,
        auto_approved,
        effective_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        approver,
        reason_code,
        reason_description,
        organization_name,
        organization_id
    FROM source_ff_xxfsam_eac_case_v
)

SELECT * FROM final