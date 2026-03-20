{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_resources_d_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_RESOURCES_D_WTCALC',
        'target_table': 'ST_TSS_RESOURCES_D_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.515340+00:00'
    }
) }}

WITH 

source_ff_tss_resources_d_wtcalc AS (
    SELECT
        bl_resource_key,
        cco_id,
        cec_id,
        fnd_user_id,
        workgroup_id,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_active_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_delete_flag,
        bl_load_id,
        resource_role_id
    FROM {{ source('raw', 'ff_tss_resources_d_wtcalc') }}
),

transformed_exp_st_tss_resources_d AS (
    SELECT
    bl_resource_key,
    cco_id,
    cec_id,
    fnd_user_id,
    workgroup_id,
    i_bl_effective_from_date,
    i_bl_effective_to_date,
    bl_active_flag,
    created_by,
    i_creation_date,
    last_updated_by,
    i_last_update_date,
    bl_created_by,
    i_bl_creation_date,
    bl_last_updated_by,
    i_bl_last_update_date,
    bl_delete_flag,
    bl_load_id,
    resource_role_id,
    IFF(ISNULL(BL_RESOURCE_KEY),-999,TO_BIGINT(BL_RESOURCE_KEY)) AS o_bl_resource_key,
    IFF(LTRIM(RTRIM(CCO_ID)) = '\N',NULL,CCO_ID) AS o_cco_id,
    IFF(LTRIM(RTRIM(CEC_ID)) = '\N',NULL,CEC_ID) AS o_cec_id,
    IFF(LTRIM(RTRIM(FND_USER_ID)) = '\N',NULL,TO_INTEGER(FND_USER_ID)) AS o_fnd_user_id,
    IFF(ISNULL(WORKGROUP_ID),-999,TO_INTEGER(WORKGROUP_ID)) AS o_workgroup_id,
    IFF(LTRIM(RTRIM(I_BL_EFFECTIVE_FROM_DATE)) = '\N',NULL,TO_DATE(I_BL_EFFECTIVE_FROM_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_from_date,
    IFF(LTRIM(RTRIM(I_BL_EFFECTIVE_TO_DATE)) = '\N',NULL,TO_DATE(I_BL_EFFECTIVE_TO_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_to_date,
    IFF(LTRIM(RTRIM(BL_ACTIVE_FLAG)) = '\N',NULL,BL_ACTIVE_FLAG) AS o_bl_active_flag,
    IFF(ISNULL(CREATED_BY),-999,TO_INTEGER(CREATED_BY)) AS o_created_by,
    IFF(ISNULL(I_CREATION_DATE),TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(I_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(ISNULL(LAST_UPDATED_BY),-999,TO_INTEGER(LAST_UPDATED_BY)) AS o_last_updated_by,
    IFF(ISNULL(I_LAST_UPDATE_DATE),TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(I_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(BL_CREATED_BY)) = '\N',NULL,BL_CREATED_BY) AS o_bl_created_by,
    IFF(ISNULL(I_BL_CREATION_DATE),TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(I_BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATED_BY)) = '\N',NULL,BL_LAST_UPDATED_BY) AS o_bl_last_updated_by,
    IFF(ISNULL(I_BL_LAST_UPDATE_DATE),TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(I_BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(BL_DELETE_FLAG)) = '\N',NULL,BL_DELETE_FLAG) AS o_bl_delete_flag,
    IFF(LTRIM(RTRIM(BL_LOAD_ID)) = '\N',NULL,TO_INTEGER(BL_LOAD_ID)) AS o_bl_load_id,
    IFF(LTRIM(RTRIM(RESOURCE_ROLE_ID)) = '\N',NULL,TO_INTEGER(RESOURCE_ROLE_ID)) AS o_resource_role_id
    FROM source_ff_tss_resources_d_wtcalc
),

final AS (
    SELECT
        bl_resource_key,
        cco_id,
        cec_id,
        fnd_user_id,
        workgroup_id,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_active_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_delete_flag,
        bl_load_id,
        resource_role_id
    FROM transformed_exp_st_tss_resources_d
)

SELECT * FROM final