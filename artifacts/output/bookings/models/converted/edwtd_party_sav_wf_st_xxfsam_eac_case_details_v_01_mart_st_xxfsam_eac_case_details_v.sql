{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_case_details_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_CASE_DETAILS_V',
        'target_table': 'ST_XXFSAM_EAC_CASE_DETAILS_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.332167+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_case_details_v AS (
    SELECT
        case_dtl_id,
        case_id,
        attribute_type,
        attribute_value,
        eac_group_id,
        eac_rule_id,
        creation_date,
        last_update_date,
        last_updated_by,
        organization_name,
        organization_id
    FROM {{ source('raw', 'ff_xxfsam_eac_case_details_v') }}
),

final AS (
    SELECT
        case_dtl_id,
        case_id,
        attribute_type,
        attribute_value,
        eac_group_id,
        eac_rule_id,
        creation_date,
        last_update_date,
        last_updated_by,
        organization_name,
        organization_id
    FROM source_ff_xxfsam_eac_case_details_v
)

SELECT * FROM final