{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_wsh_new_deliveries', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_WSH_NEW_DELIVERIES',
        'target_table': 'EL_CG1_WSH_NEW_DELIVERIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.198567+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_new_deliveries AS (
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
    FROM {{ source('raw', 'st_cg1_wsh_new_deliveries') }}
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
        global_name,
        confirm_date,
        delivered_date,
        initial_pickup_date,
        ultimate_dropoff_date
    FROM source_st_cg1_wsh_new_deliveries
)

SELECT * FROM final