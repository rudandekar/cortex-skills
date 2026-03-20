{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cisco_operating_unit_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_CISCO_OPERATING_UNIT_OOD',
        'target_table': 'N_CISCO_OPERATING_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.481054+00:00'
    }
) }}

WITH 

source_w_cisco_operating_unit AS (
    SELECT
        bk_operating_unit_name_code,
        op_unit_organization_code,
        op_unit_effective_start_date,
        op_unit_effective_end_date,
        bk_bus_entity_org_name_code,
        sk_organization_id_int,
        operating_unit_short_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        operating_unit_type_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cisco_operating_unit') }}
),

final AS (
    SELECT
        bk_operating_unit_name_code,
        op_unit_organization_code,
        op_unit_effective_start_date,
        op_unit_effective_end_date,
        bk_bus_entity_org_name_code,
        sk_organization_id_int,
        operating_unit_short_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        operating_unit_type_cd
    FROM source_w_cisco_operating_unit
)

SELECT * FROM final