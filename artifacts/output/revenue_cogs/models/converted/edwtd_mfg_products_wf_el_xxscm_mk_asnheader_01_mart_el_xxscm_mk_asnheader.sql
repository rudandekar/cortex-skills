{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxscm_mk_asnheader', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_XXSCM_MK_ASNHEADER',
        'target_table': 'EL_XXSCM_MK_ASNHEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.854585+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_mk_asnheader_i AS (
    SELECT
        message_3b2_id,
        shipment_number,
        freight_carrier,
        service_level,
        vehicle_number,
        frm_point,
        to_point,
        ship_date,
        bol_no,
        ship_via,
        shipment_note,
        tracking_number,
        partner_duns_no,
        partner_code,
        error_message,
        error_code,
        b2b_process_status,
        reason_code,
        transmission_date,
        process_status,
        sj_process_status,
        ack_status,
        func_ack_creation_date,
        func_ack_last_updated_date,
        email_notification,
        cisco_duns_no,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        system_time_zone,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_mk_asnheader_i') }}
),

final AS (
    SELECT
        message_3b2_id,
        shipment_number,
        freight_carrier,
        service_level,
        vehicle_number,
        frm_point,
        to_point,
        ship_date,
        bol_no,
        ship_via,
        shipment_note,
        tracking_number,
        partner_duns_no,
        partner_code,
        error_message,
        error_code,
        b2b_process_status,
        reason_code,
        transmission_date,
        process_status,
        sj_process_status,
        ack_status,
        func_ack_creation_date,
        func_ack_last_updated_date,
        email_notification,
        cisco_duns_no,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        system_time_zone,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_st_cg1_xxscm_mk_asnheader_i
)

SELECT * FROM final