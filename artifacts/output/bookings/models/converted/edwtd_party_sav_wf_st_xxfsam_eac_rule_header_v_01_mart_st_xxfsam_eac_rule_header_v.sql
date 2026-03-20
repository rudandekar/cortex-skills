{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_rule_header_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_RULE_HEADER_V',
        'target_table': 'ST_XXFSAM_EAC_RULE_HEADER_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.788889+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_rule_header_v AS (
    SELECT
        rule_id,
        rule_start_date,
        rule_end_date,
        eac_group_id,
        rule_header_creation_date,
        rule_header_created_by,
        rule_header_last_updated_by,
        rule_header_last_updated_date,
        rule_detail_id,
        attribute_name,
        attribute_value,
        start_date,
        end_date,
        rule_attr_created_by,
        rule_attr_creation_date,
        rule_attr_last_update_date,
        rule_attr_last_updated_by,
        organization_name,
        organization_id,
        header_status,
        rule_status,
        attribute_status,
        attribute_value_status,
        profile_id,
        profile_name,
        publish_indicator
    FROM {{ source('raw', 'ff_xxfsam_eac_rule_header_v') }}
),

final AS (
    SELECT
        rule_id,
        rule_start_date,
        rule_end_date,
        eac_group_id,
        rule_header_creation_date,
        rule_header_created_by,
        rule_header_last_updated_date,
        rule_header_last_updated_by,
        rule_detail_id,
        attribute_name,
        attribute_value,
        start_date,
        end_date,
        rule_attr_created_by,
        rule_attr_creation_date,
        rule_attr_last_update_date,
        rule_attr_last_updated_by,
        organization_name,
        organization_id,
        header_status,
        rule_status,
        attribute_status,
        attribute_value_status,
        profile_id,
        profile_name,
        publish_indicator
    FROM source_ff_xxfsam_eac_rule_header_v
)

SELECT * FROM final