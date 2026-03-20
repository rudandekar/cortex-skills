{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_base_caseactivity_log_enhn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_BASE_CASEACTIVITY_LOG_ENHN',
        'target_table': 'WI_BASE_CASEACTIVITY_LOG_ENHN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.487959+00:00'
    }
) }}

WITH 

source_st_base_caseactivity_log_enhn AS (
    SELECT
        dw_case_activity_key,
        incident_id,
        incident_number,
        casenumber,
        incident_status_id,
        incident_status,
        current_severity,
        current_owner_cco_id,
        current_owner_group_id,
        current_owner_group_name,
        current_contract_key,
        current_contract_svc_line_id,
        current_dw_workgroup_key,
        current_serial_number,
        activity_code,
        activity_name,
        activity_date,
        creation_date,
        initial_severity_id,
        activity_addnl_info,
        res_role,
        delivery_channel_type,
        complexity,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wg_service_type,
        sr_type,
        note_type,
        pre_dw_crnt_keyword_key,
        current_wg_service_type
    FROM {{ source('raw', 'st_base_caseactivity_log_enhn') }}
),

source_wi_base_caseactivity_log_enhn AS (
    SELECT
        dw_case_activity_key,
        incident_id,
        incident_number,
        casenumber,
        incident_status_id,
        incident_status,
        current_severity,
        current_owner_cco_id,
        current_owner_group_id,
        current_owner_group_name,
        current_contract_key,
        current_contract_svc_line_id,
        current_dw_workgroup_key,
        current_serial_number,
        activity_name,
        activity_date,
        creation_date,
        initial_severity_id,
        activity_addnl_info,
        res_role,
        delivery_channel,
        complexity,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wg_service_type,
        sr_type,
        note_type,
        dw_crnt_keyword_key,
        current_wg_service_type,
        activity_code,
        record_hash,
        dw_delete_flag
    FROM {{ source('raw', 'wi_base_caseactivity_log_enhn') }}
),

final AS (
    SELECT
        dw_case_activity_key,
        incident_id,
        incident_number,
        casenumber,
        incident_status_id,
        incident_status,
        current_severity,
        current_owner_cco_id,
        current_owner_group_id,
        current_owner_group_name,
        current_contract_key,
        current_contract_svc_line_id,
        current_dw_workgroup_key,
        current_serial_number,
        activity_name,
        activity_date,
        creation_date,
        initial_severity_id,
        activity_addnl_info,
        res_role,
        delivery_channel,
        complexity,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wg_service_type,
        sr_type,
        note_type,
        dw_crnt_keyword_key,
        current_wg_service_type,
        activity_code,
        record_hash,
        dw_delete_flag
    FROM source_wi_base_caseactivity_log_enhn
)

SELECT * FROM final