{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_po_control_rules', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PO_CONTROL_RULES',
        'target_table': 'EL_PO_CONTROL_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.140971+00:00'
    }
) }}

WITH 

source_st_mf_po_control_rules AS (
    SELECT
        batch_id,
        amount_limit,
        attribute1,
        attribute10,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute_category,
        control_group_id,
        control_rule_id,
        created_by,
        creation_date,
        ges_update_date,
        global_name,
        inactive_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        location_id,
        object_code,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        rule_type_code,
        segment2_high,
        segment2_low,
        segment3_high,
        segment3_low,
        segment4_high,
        segment4_low,
        segment5_high,
        segment5_low,
        segment6_high,
        segment6_low,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_po_control_rules') }}
),

final AS (
    SELECT
        amount_limit,
        attribute1,
        attribute10,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute_category,
        control_group_id,
        control_rule_id,
        creation_date,
        ges_update_date,
        global_name,
        inactive_date,
        last_update_date,
        location_id,
        object_code,
        rule_type_code,
        segment2_high,
        segment2_low,
        segment3_high,
        segment3_low,
        segment4_high,
        segment4_low,
        segment5_high,
        segment5_low,
        segment6_high,
        segment6_low
    FROM source_st_mf_po_control_rules
)

SELECT * FROM final