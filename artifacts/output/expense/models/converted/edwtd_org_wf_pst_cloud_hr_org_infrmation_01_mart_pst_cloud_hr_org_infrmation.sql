{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_hr_org_infrmation', 'batch', 'edwtd_org'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_HR_ORG_INFRMATION',
        'target_table': 'PST_CLOUD_HR_ORG_INFRMATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.136661+00:00'
    }
) }}

WITH 

source_cbm_organization_units AS (
    SELECT
        created_by,
        creation_date,
        effective_start_date,
        effective_end_date,
        org_information8,
        org_information4,
        org_information6,
        business_group_id,
        org_information7,
        business_unit_id,
        last_update_date,
        last_updated_by,
        org_information2,
        location_id,
        org_information1,
        business_unit_name,
        org_information3,
        status,
        ledger_currency_code,
        ledger_description,
        ledger_id,
        ledger_name,
        sla_accounting_method_code,
        sla_accounting_method_type,
        legal_entity_id,
        legal_entity_name,
        location_code,
        location_name,
        org_information5,
        org_information9,
        org_information_id,
        type,
        set_id,
        set_name,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_organization_units') }}
),

final AS (
    SELECT
        created_by,
        creation_date,
        effective_start_date,
        effective_end_date,
        org_information8,
        org_information4,
        org_information6,
        business_group_id,
        org_information7,
        business_unit_id,
        last_update_date,
        last_updated_by,
        org_information2,
        location_id,
        org_information1,
        business_unit_name,
        org_information3,
        status,
        ledger_currency_code,
        ledger_description,
        ledger_id,
        ledger_name,
        sla_accounting_method_code,
        sla_accounting_method_type,
        legal_entity_id,
        legal_entity_name,
        location_code,
        location_name,
        org_information5,
        org_information9,
        org_information_id,
        type1,
        set_id,
        set_name,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_organization_units
)

SELECT * FROM final