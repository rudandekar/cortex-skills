{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_order_headers_f_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_ORDER_HEADERS_F_WTCALC',
        'target_table': 'ST_TSS_ORDER_HEADERS_F_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.745956+00:00'
    }
) }}

WITH 

source_ff_tss_order_headers_f_wtcalc AS (
    SELECT
        bl_order_key,
        creator_workgroup_key,
        creator_resource_key,
        order_type_lkp_key,
        incident_id,
        casenumber,
        order_number,
        creation_date,
        bl_last_update_date
    FROM {{ source('raw', 'ff_tss_order_headers_f_wtcalc') }}
),

transformed_ex_st_tss_order_headers_f_tac AS (
    SELECT
    bl_order_key,
    creator_workgroup_key,
    creator_resource_key,
    order_type_lkp_key,
    incident_id,
    casenumber,
    order_number,
    creation_date,
    bl_last_update_date,
    IFF(LTRIM(RTRIM(bl_ORDER_key)) = '\N',-999,TO_INTEGER(bl_ORDER_key)) AS o_bl_order_key,
    IFF(LTRIM(RTRIM(CREATOR_WORKGROUP_KEY)) = '\N',NULL,CREATOR_WORKGROUP_KEY) AS o_creator_workgroup_key,
    IFF(LTRIM(RTRIM(creator_resource_key))='\N',-999,TO_BIGINT(creator_resource_key)) AS o_creator_resource_key,
    IFF(LTRIM(RTRIM(order_type_lkp_key)) = '\N',-999,TO_INTEGER(order_type_lkp_key)) AS o_order_type_lkp_key,
    IFF(LTRIM(RTRIM(INCIDENT_ID)) = '\N',-999,TO_INTEGER(INCIDENT_ID)) AS o_incident_id,
    IFF(LTRIM(RTRIM(casenumber))='\N',-999,TO_BIGINT(casenumber)) AS o_casenumber,
    IFF(LTRIM(RTRIM(order_number))='\N',-999,TO_BIGINT(order_number)) AS o_order_number,
    IFF(LTRIM(RTRIM(creation_date))='\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(creation_date,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(bl_last_update_date))='\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(bl_last_update_date,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date
    FROM source_ff_tss_order_headers_f_wtcalc
),

final AS (
    SELECT
        bl_order_key,
        creator_workgroup_key,
        creator_resource_key,
        order_type_lkp_key,
        incident_id,
        casenumber,
        order_number,
        creation_date,
        bl_last_update_date
    FROM transformed_ex_st_tss_order_headers_f_tac
)

SELECT * FROM final