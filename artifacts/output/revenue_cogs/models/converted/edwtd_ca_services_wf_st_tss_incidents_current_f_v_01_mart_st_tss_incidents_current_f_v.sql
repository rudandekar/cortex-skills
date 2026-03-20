{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_incidents_current_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_INCIDENTS_CURRENT_F_V',
        'target_table': 'ST_TSS_INCIDENTS_CURRENT_F_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.442196+00:00'
    }
) }}

WITH 

source_ff_tss_incidents_current_f_v AS (
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
    FROM {{ source('raw', 'ff_tss_incidents_current_f_v') }}
),

transformed_exptrans AS (
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
    casenumber,
    IFF(LTRIM(RTRIM(INCIDENT_NUMBER)) = '\N',NULL,INCIDENT_NUMBER) AS o_incident_number,
    IFF(LTRIM(RTRIM(INCIDENT_STATUS)) = '\N',NULL,INCIDENT_STATUS) AS o_incident_status,
    IFF(LTRIM(RTRIM(BL_CUSTOMER_KEY)) = '\N',NULL,TO_BIGINT(BL_CUSTOMER_KEY)) AS o_bl_customer_key,
    IFF(LTRIM(RTRIM(INCIDENT_TYPE_KEY)) = '\N',NULL,TO_BIGINT(INCIDENT_TYPE_KEY)) AS o_incident_type_key,
    IFF(LTRIM(RTRIM(INCIDENT_ID)) = '\N',NULL,TO_BIGINT(INCIDENT_ID)) AS o_incident_id,
    IFF(LTRIM(RTRIM(HW_VERSION_ACT_ID)) = '\N',NULL,TO_BIGINT(HW_VERSION_ACT_ID)) AS o_hw_version_act_id,
    IFF(LTRIM(RTRIM(CREATION_CALENDAR_KEY)) = '\N',NULL,TO_BIGINT(CREATION_CALENDAR_KEY)) AS o_creation_calendar_key,
    IFF(LTRIM(RTRIM(INVENTORY_ITEM_ID)) = '\N',NULL,TO_BIGINT(INVENTORY_ITEM_ID)) AS o_inventory_item_id,
    IFF(LTRIM(RTRIM(CLOSED_DATE)) = '\N',NULL,TO_DATE(CLOSED_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_closed_date,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(BL_CREATION_DATE)) = '\N',NULL,TO_DATE(BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(casenumber)) = '\N',NULL,TO_DECIMAL(casenumber,0)) AS o_casenumber
    FROM source_ff_tss_incidents_current_f_v
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
        casenumber
    FROM transformed_exptrans
)

SELECT * FROM final