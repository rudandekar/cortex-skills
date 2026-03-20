{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_workgroups_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_WORKGROUPS_D',
        'target_table': 'ST_TSS_WORKGROUPS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.422926+00:00'
    }
) }}

WITH 

source_ff_tss_workgroups_d AS (
    SELECT
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        workgroup_desc,
        wkg_mgr_resource_id,
        wkg_mgr_first_name,
        wkg_mgr_last_name,
        wkg_mgr_email,
        sub_region_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        service_line_name,
        service_type,
        creation_date,
        last_update_date,
        bl_last_update_date,
        tech_id,
        active_flag,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_to_date,
        bl_effective_from_date
    FROM {{ source('raw', 'ff_tss_workgroups_d') }}
),

transformed_expr AS (
    SELECT
    bl_workgroup_key,
    workgroup_id,
    workgroup_name,
    workgroup_desc,
    wkg_mgr_resource_id,
    wkg_mgr_first_name,
    wkg_mgr_last_name,
    wkg_mgr_email,
    sub_region_name,
    theater_name,
    delivery_channel_type,
    outsourcer_name,
    service_line_name,
    service_type,
    creation_date,
    last_update_date,
    bl_last_update_date,
    tech_id,
    active_flag,
    bl_active_flag,
    bl_delete_flag,
    bl_effective_to_date,
    bl_effective_from_date,
    IFF(LTRIM(RTRIM(BL_WORKGROUP_KEY))='\N',-999,TO_INTEGER(BL_WORKGROUP_KEY)) AS o_bl_workgroup_key,
    IFF(LTRIM(RTRIM(WORKGROUP_ID))='\N',-999,TO_INTEGER(WORKGROUP_ID)) AS o_workgroup_id,
    IFF(LTRIM(RTRIM(WORKGROUP_NAME))='\N',NULL,WORKGROUP_NAME) AS o_workgroup_name,
    IFF(LTRIM(RTRIM(WORKGROUP_DESC))='\N',NULL,WORKGROUP_DESC) AS o_workgroup_desc,
    IFF(LTRIM(RTRIM(WKG_MGR_RESOURCE_ID))='\N',NULL,TO_INTEGER(WKG_MGR_RESOURCE_ID)) AS o_wkg_mgr_resource_id,
    IFF(LTRIM(RTRIM(WKG_MGR_FIRST_NAME))='\N',NULL,WKG_MGR_FIRST_NAME) AS o_wkg_mgr_first_name,
    IFF(LTRIM(RTRIM(WKG_MGR_LAST_NAME))='\N',NULL,WKG_MGR_LAST_NAME) AS o_wkg_mgr_last_name,
    IFF(LTRIM(RTRIM(WKG_MGR_EMAIL))='\N',NULL,WKG_MGR_EMAIL) AS o_wkg_mgr_email,
    IFF(LTRIM(RTRIM(SUB_REGION_NAME))='\N',NULL,SUB_REGION_NAME) AS o_sub_region_name,
    IFF(LTRIM(RTRIM(THEATER_NAME))='\N',NULL,THEATER_NAME) AS o_theater_name,
    IFF(LTRIM(RTRIM(DELIVERY_CHANNEL_TYPE))='\N',NULL,DELIVERY_CHANNEL_TYPE) AS o_delivery_channel_type,
    IFF(LTRIM(RTRIM(OUTSOURCER_NAME))='\N',NULL,OUTSOURCER_NAME) AS o_outsourcer_name,
    IFF(LTRIM(RTRIM(SERVICE_LINE_NAME))='\N',NULL,SERVICE_LINE_NAME) AS o_service_line_name,
    IFF(LTRIM(RTRIM(SERVICE_TYPE))='\N',NULL,SERVICE_TYPE) AS o_service_type,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500/01/01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(TECH_ID))='\N',-999,TO_INTEGER(TECH_ID)) AS o_tech_id,
    IFF(LTRIM(RTRIM(ACTIVE_FLAG))='\N',NULL,ACTIVE_FLAG) AS o_active_flag,
    IFF(LTRIM(RTRIM(BL_ACTIVE_FLAG))='\N',NULL,BL_ACTIVE_FLAG) AS o_bl_active_flag,
    IFF(LTRIM(RTRIM(BL_DELETE_FLAG))='\N',NULL,BL_DELETE_FLAG) AS o_bl_delete_flag,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_TO_DATE)) = '\N',NULL,TO_DATE(BL_EFFECTIVE_TO_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_to_date,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_FROM_DATE)) = '\N',NULL,TO_DATE(BL_EFFECTIVE_FROM_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_from_date
    FROM source_ff_tss_workgroups_d
),

final AS (
    SELECT
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        workgroup_desc,
        wkg_mgr_resource_id,
        wkg_mgr_first_name,
        wkg_mgr_last_name,
        wkg_mgr_email,
        sub_region_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        service_line_name,
        service_type,
        creation_date,
        last_update_date,
        bl_last_update_date,
        tech_id,
        active_flag,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_to_date,
        bl_effective_from_date
    FROM transformed_expr
)

SELECT * FROM final