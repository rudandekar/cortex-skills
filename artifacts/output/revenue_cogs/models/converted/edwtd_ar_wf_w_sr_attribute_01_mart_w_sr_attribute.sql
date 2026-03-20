{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_attribute', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_SR_ATTRIBUTE',
        'target_table': 'W_SR_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.439489+00:00'
    }
) }}

WITH 

source_n_tss_incidents_current_f AS (
    SELECT
        incident_id,
        incident_number,
        creation_calendar_key,
        inventory_item_id,
        bl_customer_key,
        bl_last_update_date,
        current_serial_number,
        creation_date,
        transaction_date,
        closed_date,
        hw_version_id,
        create_datetime,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_tss_incidents_current_f') }}
),

final AS (
    SELECT
        incident_number,
        cust_theater,
        cust_country,
        code,
        creation_date,
        closed_date,
        product_id,
        node_level_3_name,
        item_name
    FROM source_n_tss_incidents_current_f
)

SELECT * FROM final