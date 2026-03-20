{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_incidents_current_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_INCIDENTS_CURRENT_F_V',
        'target_table': 'EL_TSS_INCIDENTS_CURRENT_F_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.714876+00:00'
    }
) }}

WITH 

source_st_tss_incidents_current_f_v AS (
    SELECT
        incident_number,
        incident_status,
        bl_customer_key,
        incident_type_key,
        incident_id,
        hw_version_act_id,
        creation_calendar_key,
        inventory_item_id,
        closed_date,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        casenumber
    FROM {{ source('raw', 'st_tss_incidents_current_f_v') }}
),

final AS (
    SELECT
        incident_number,
        incident_status,
        bl_customer_key,
        incident_type_key,
        incident_id,
        hw_version_act_id,
        creation_calendar_key,
        inventory_item_id,
        closed_date,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        casenumber
    FROM source_st_tss_incidents_current_f_v
)

SELECT * FROM final