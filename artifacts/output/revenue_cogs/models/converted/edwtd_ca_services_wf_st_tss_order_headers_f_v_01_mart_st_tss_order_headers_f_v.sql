{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_order_headers_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_ORDER_HEADERS_F_V',
        'target_table': 'ST_TSS_ORDER_HEADERS_F_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.858867+00:00'
    }
) }}

WITH 

source_ff_tss_order_headers_f_v AS (
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
    FROM {{ source('raw', 'ff_tss_order_headers_f_v') }}
),

transformed_exptrans AS (
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
    order_source_lkp_key,
    IFF(LTRIM(RTRIM(ORDER_NUMBER)) = '\N',NULL,TO_BIGINT(ORDER_NUMBER)) AS o_order_number,
    IFF(LTRIM(RTRIM(FAILURE_CODE)) = '\N',NULL,FAILURE_CODE) AS o_failure_code,
    IFF(LTRIM(RTRIM(HEADER_ID)) = '\N',NULL,TO_BIGINT(HEADER_ID)) AS o_header_id,
    IFF(LTRIM(RTRIM(CONTRACTUAL_SERVICE_LEVEL_KEY)) = '\N',NULL,TO_BIGINT(CONTRACTUAL_SERVICE_LEVEL_KEY)) AS o_contractual_service_level_key,
    IFF(LTRIM(RTRIM(REQUESTED_SERVICE_LEVEL_KEY)) = '\N',NULL,TO_BIGINT(REQUESTED_SERVICE_LEVEL_KEY)) AS o_requested_service_level_key,
    IFF(LTRIM(RTRIM(ORDER_TYPE_LKP_KEY)) = '\N',NULL,TO_BIGINT(ORDER_TYPE_LKP_KEY)) AS o_order_type_lkp_key,
    IFF(LTRIM(RTRIM(INCIDENT_ID)) = '\N',NULL,TO_BIGINT(INCIDENT_ID)) AS o_incident_id,
    IFF(LTRIM(RTRIM(TRANSACTION_SOURCE)) = '\N',NULL,TRANSACTION_SOURCE) AS o_transaction_source,
    IFF(LTRIM(RTRIM(CONTRACT_KEY)) = '\N',NULL,TO_BIGINT(CONTRACT_KEY)) AS o_contract_key,
    IFF(LTRIM(RTRIM(CUST_SITE_KEY)) = '\N',NULL,TO_BIGINT(CUST_SITE_KEY)) AS o_cust_site_key,
    IFF(LTRIM(RTRIM(ORDER_CREATION_DATE)) = '\N',NULL,TO_DATE(ORDER_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_order_creation_date,
    IFF(LTRIM(RTRIM(CREATION_DATE_CALENDER_KEY)) = '\N',NULL,TO_BIGINT(CREATION_DATE_CALENDER_KEY)) AS o_creation_date_calender_key,
    IFF(LTRIM(RTRIM(CREATOR_RESOURCE_KEY)) = '\N',NULL,TO_BIGINT(CREATOR_RESOURCE_KEY)) AS o_creator_resource_key,
    IFF(LTRIM(RTRIM(CREATOR_WORKGROUP_KEY)) = '\N',NULL,TO_BIGINT(CREATOR_WORKGROUP_KEY)) AS o_creator_workgroup_key,
    IFF(LTRIM(RTRIM(BL_CREATION_DATE)) = '\N',NULL,TO_DATE(BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(casenumber)) = '\N',NULL,TO_DECIMAL(casenumber,0)) AS o_casenumber,
    IFF(LTRIM(RTRIM(ORDER_SOURCE_LKP_KEY)) = '\N',NULL,TO_DECIMAL(ORDER_SOURCE_LKP_KEY,0)) AS o_order_source_lkp_key
    FROM source_ff_tss_order_headers_f_v
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
        casenumber,
        order_source_lkp_key
    FROM transformed_exptrans
)

SELECT * FROM final