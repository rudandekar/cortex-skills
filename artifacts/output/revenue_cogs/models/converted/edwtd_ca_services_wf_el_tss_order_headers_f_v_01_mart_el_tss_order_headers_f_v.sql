{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_order_headers_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_ORDER_HEADERS_F_V',
        'target_table': 'EL_TSS_ORDER_HEADERS_F_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.674741+00:00'
    }
) }}

WITH 

source_st_tss_order_headers_f_v AS (
    SELECT
        order_number,
        failure_code,
        header_id,
        contractual_service_level_key,
        requested_service_level_key,
        order_type_lkp_key,
        incident_id,
        transaction_source,
        contract_key,
        cust_site_key,
        order_creation_date,
        creation_date_calender_key,
        creator_resource_key,
        creator_workgroup_key,
        bl_creation_date,
        bl_last_update_date,
        creation_date,
        last_update_date,
        casenumber,
        order_source_lkp_key
    FROM {{ source('raw', 'st_tss_order_headers_f_v') }}
),

final AS (
    SELECT
        order_number,
        failure_code,
        header_id,
        contractual_service_level_key,
        requested_service_level_key,
        order_type_lkp_key,
        incident_id,
        transaction_source,
        contract_key,
        cust_site_key,
        order_creation_date,
        creation_date_calender_key,
        creator_resource_key,
        creator_workgroup_key,
        bl_creation_date,
        bl_last_update_date,
        creation_date,
        last_update_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        casenumber,
        order_source_lkp_key
    FROM source_st_tss_order_headers_f_v
)

SELECT * FROM final