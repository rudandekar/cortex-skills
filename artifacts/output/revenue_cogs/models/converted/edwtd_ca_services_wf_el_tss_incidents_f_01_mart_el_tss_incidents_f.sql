{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_incidents_f', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_INCIDENTS_F',
        'target_table': 'EL_TSS_INCIDENTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.356860+00:00'
    }
) }}

WITH 

source_st_tss_incidents_f AS (
    SELECT
        activity_addnl_info,
        activity_code,
        activity_date,
        activity_name,
        b2b_flag,
        bl_customer_key,
        bl_incident_key,
        bl_last_update_date,
        casenumber,
        complexity,
        creation_date,
        current_contract_key,
        current_contract_svc_line_id,
        current_owner_cco_id,
        current_owner_group_id,
        current_owner_group_name,
        current_serial_number,
        current_severity,
        incident_id,
        incident_number,
        incident_status,
        incident_status_id,
        incident_type,
        incident_type_key,
        initial_severity_id,
        last_update_date,
        last_updated_by,
        note_type,
        problem_code,
        updated_cot_tech_key
    FROM {{ source('raw', 'st_tss_incidents_f') }}
),

transformed_expr AS (
    SELECT
    activity_addnl_info,
    activity_code,
    activity_date,
    activity_name,
    b2b_flag,
    bl_customer_key,
    bl_incident_key,
    bl_last_update_date,
    casenumber,
    complexity,
    creation_date,
    current_contract_key,
    current_contract_svc_line_id,
    current_owner_cco_id,
    current_owner_group_id,
    current_owner_group_name,
    current_serial_number,
    current_severity,
    incident_id,
    incident_number,
    incident_status,
    incident_status_id,
    incident_type,
    incident_type_key,
    initial_severity_id,
    last_update_date,
    last_updated_by,
    note_type,
    problem_code,
    updated_cot_tech_key
    FROM source_st_tss_incidents_f
),

final AS (
    SELECT
        activity_addnl_info,
        activity_code,
        activity_date,
        activity_name,
        b2b_flag,
        bl_customer_key,
        bl_incident_key,
        bl_last_update_date,
        casenumber,
        complexity,
        creation_date,
        current_contract_key,
        current_contract_svc_line_id,
        current_owner_cco_id,
        current_owner_group_id,
        current_owner_group_name,
        current_serial_number,
        current_severity,
        incident_id,
        incident_number,
        incident_status,
        incident_status_id,
        incident_type,
        incident_type_key,
        initial_severity_id,
        last_update_date,
        last_updated_by,
        note_type,
        problem_code,
        updated_cot_tech_key
    FROM transformed_expr
)

SELECT * FROM final