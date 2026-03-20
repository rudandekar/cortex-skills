{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_wsh_new_deliveries', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_WSH_NEW_DELIVERIES',
        'target_table': 'ST_CG1_WSH_NEW_DELIVERIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.774439+00:00'
    }
) }}

WITH 

source_cg1_wsh_new_deliveries AS (
    SELECT
        delivery_id,
        name,
        planned_flag,
        status_code,
        creation_date,
        asn_date_sent,
        asn_status_code,
        ges_update_date,
        global_name,
        delivered_date,
        confirm_date,
        initial_pickup_date,
        ultimate_dropoff_date,
        source_dml_type,
        source_commit_time,
        trail_file_name,
        refresh_datetime
    FROM {{ source('raw', 'cg1_wsh_new_deliveries') }}
),

final AS (
    SELECT
        delivery_id,
        name,
        planned_flag,
        status_code,
        creation_date,
        asn_date_sent,
        asn_status_code,
        delivered_date,
        confirm_date,
        initial_pickup_date,
        ultimate_dropoff_date,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_wsh_new_deliveries
)

SELECT * FROM final