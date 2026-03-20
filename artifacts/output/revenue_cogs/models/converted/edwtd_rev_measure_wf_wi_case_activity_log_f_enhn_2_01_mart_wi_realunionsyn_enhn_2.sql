{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_case_activity_log_f_enhn_2', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CASE_ACTIVITY_LOG_F_ENHN_2',
        'target_table': 'WI_REALUNIONSYN_ENHN_2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.618539+00:00'
    }
) }}

WITH 

source_wi_realunionsyn_enhn_2 AS (
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
        delivery_channel,
        complexity,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wg_service_type,
        sr_type,
        note_type,
        current_wg_service_type,
        dw_crnt_keyword_key
    FROM {{ source('raw', 'wi_realunionsyn_enhn_2') }}
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
        activity_code,
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
        current_wg_service_type,
        dw_crnt_keyword_key
    FROM source_wi_realunionsyn_enhn_2
)

SELECT * FROM final