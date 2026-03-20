{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_tac_center_lines_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_TAC_CENTER_LINES_D',
        'target_table': 'ST_TSS_TAC_CENTER_LINES_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.233209+00:00'
    }
) }}

WITH 

source_ff_tss_tac_center_lines_d AS (
    SELECT
        bl_tac_center_line_key,
        tac_center_line_id,
        tac_center_id,
        resource_site_id,
        workgroup_id,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_source_system,
        bl_load_id
    FROM {{ source('raw', 'ff_tss_tac_center_lines_d') }}
),

transformed_ex_st_tss_tac_center_lines_d AS (
    SELECT
    bl_tac_center_line_key,
    tac_center_line_id,
    tac_center_id,
    resource_site_id,
    workgroup_id,
    enabled_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    bl_active_flag,
    bl_delete_flag,
    bl_effective_from_date,
    bl_effective_to_date,
    bl_creation_date,
    bl_created_by,
    bl_last_update_date,
    bl_last_updated_by,
    bl_source_system,
    bl_load_id,
    IFF(LTRIM(RTRIM(BL_TAC_CENTER_LINE_KEY)) = '\N',-999,TO_INTEGER(BL_TAC_CENTER_LINE_KEY)) AS o_bl_tac_center_line_key,
    IFF(LTRIM(RTRIM(TAC_CENTER_LINE_ID)) = '\N',-999,TO_INTEGER(TAC_CENTER_LINE_ID)) AS o_tac_center_line_id,
    IFF(LTRIM(RTRIM(TAC_CENTER_ID))='\N',-999,TO_INTEGER(TAC_CENTER_ID)) AS o_tac_center_id,
    IFF(LTRIM(RTRIM(RESOURCE_SITE_ID))='\N',-999,TO_INTEGER(RESOURCE_SITE_ID)) AS o_resource_site_id,
    IFF(LTRIM(RTRIM(WORKGROUP_ID))='\N',-999,TO_INTEGER(WORKGROUP_ID)) AS o_workgroup_id,
    IFF(LTRIM(RTRIM(ENABLED_FLAG))='\N',NULL,ENABLED_FLAG) AS o_enabled_flag,
    IFF(LTRIM(RTRIM(CREATED_BY))='\N',-999,TO_INTEGER(CREATED_BY)) AS o_created_by,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(LAST_UPDATED_BY))='\N',-999,TO_INTEGER(LAST_UPDATED_BY)) AS o_last_updated_by,
    IFF(LTRIM(RTRIM(LAST_UPDATE_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(BL_ACTIVE_FLAG)) = '\N',NULL,BL_ACTIVE_FLAG) AS o_bl_active_flag,
    IFF(LTRIM(RTRIM(BL_DELETE_FLAG)) = '\N',NULL,BL_DELETE_FLAG) AS o_bl_delete_flag,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_FROM_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_EFFECTIVE_FROM_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_from_date,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_TO_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_EFFECTIVE_TO_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_to_date,
    IFF(LTRIM(RTRIM(BL_CREATION_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_CREATED_BY)) = '\N',NULL,BL_CREATED_BY) AS o_bl_created_by,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATED_BY)) = '\N',NULL,BL_LAST_UPDATED_BY) AS o_bl_last_updated_by,
    IFF(LTRIM(RTRIM(BL_SOURCE_SYSTEM)) = '\N',NULL,BL_SOURCE_SYSTEM) AS o_bl_source_system,
    IFF(LTRIM(RTRIM(BL_LOAD_ID))='\N',-999,TO_INTEGER(BL_LOAD_ID)) AS o_bl_load_id
    FROM source_ff_tss_tac_center_lines_d
),

final AS (
    SELECT
        bl_tac_center_line_key,
        tac_center_line_id,
        tac_center_id,
        resource_site_id,
        workgroup_id,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_source_system,
        bl_load_id
    FROM transformed_ex_st_tss_tac_center_lines_d
)

SELECT * FROM final