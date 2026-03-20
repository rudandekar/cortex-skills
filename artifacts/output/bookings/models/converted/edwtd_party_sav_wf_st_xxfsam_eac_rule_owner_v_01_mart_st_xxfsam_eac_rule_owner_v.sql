{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_rule_owner_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_RULE_OWNER_V',
        'target_table': 'ST_XXFSAM_EAC_RULE_OWNER_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.723585+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_rule_owner_v AS (
    SELECT
        rule_id,
        rule_start_date,
        rule_end_date,
        eac_group_id,
        rule_header_creation_date,
        rule_header_created_by,
        rule_header_last_updated_date,
        rule_header_last_updated_by,
        rule_owner_dtl_id,
        rule_owner_id,
        rule_node_id,
        territory_type,
        rule_owner_start_date,
        rule_owner_end_date,
        rule_owner_creation_date,
        rule_owner_created_by,
        rule_owner_last_updated_date,
        rule_owner_last_updated_by,
        organization_name,
        organization_id,
        header_status,
        rule_status,
        owner_status
    FROM {{ source('raw', 'ff_xxfsam_eac_rule_owner_v') }}
),

final AS (
    SELECT
        rule_id,
        rule_owner_dtl_id,
        rule_owner_id,
        rule_node_id,
        territory_type,
        rule_owner_start_date,
        rule_owner_end_date,
        rule_owner_creation_date,
        rule_owner_created_by,
        rule_owner_last_updated_date,
        rule_owner_last_updated_by,
        organization_name,
        organization_id,
        owner_status
    FROM source_ff_xxfsam_eac_rule_owner_v
)

SELECT * FROM final