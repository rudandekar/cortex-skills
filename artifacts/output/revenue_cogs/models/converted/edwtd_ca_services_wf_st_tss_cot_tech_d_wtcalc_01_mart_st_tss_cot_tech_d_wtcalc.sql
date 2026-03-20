{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_cot_tech_d_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_COT_TECH_D_WTCALC',
        'target_table': 'ST_TSS_COT_TECH_D_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.720991+00:00'
    }
) }}

WITH 

source_ff_tss_cot_technologies_d_wtcalc AS (
    SELECT
        bl_cot_technology_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        tech_active_flag,
        sub_tech_active_flag,
        bl_creation_date,
        bl_last_update_date
    FROM {{ source('raw', 'ff_tss_cot_technologies_d_wtcalc') }}
),

transformed_expr AS (
    SELECT
    bl_cot_technology_key,
    tech_id,
    tech_name,
    sub_tech_id,
    sub_tech_name,
    tech_active_flag,
    sub_tech_active_flag,
    bl_creation_date,
    bl_last_update_date,
    IFF(LTRIM(RTRIM(BL_COT_TECHNOLOGY_KEY)) = '\N',-999,TO_INTEGER(BL_COT_TECHNOLOGY_KEY)) AS o_bl_cot_technology_key,
    IFF(LTRIM(RTRIM(TECH_ID)) = '\N',NULL,TO_INTEGER(TECH_ID)) AS o_tech_id,
    IFF(LTRIM(RTRIM(TECH_NAME))='\N',NULL,TECH_NAME) AS o_tech_name,
    IFF(LTRIM(RTRIM(SUB_TECH_ID)) = '\N',NULL,TO_INTEGER(SUB_TECH_ID)) AS o_sub_tech_id,
    IFF(LTRIM(RTRIM(SUB_TECH_NAME))='\N',NULL,SUB_TECH_NAME) AS o_sub_tech_name,
    IFF(LTRIM(RTRIM(TECH_ACTIVE_FLAG))='\N',NULL,TECH_ACTIVE_FLAG) AS o_tech_active_flag,
    IFF(LTRIM(RTRIM(SUB_TECH_ACTIVE_FLAG))='\N',NULL,SUB_TECH_ACTIVE_FLAG) AS o_sub_tech_active_flag,
    IFF(LTRIM(RTRIM(BL_CREATION_DATE)) = '\N',NULL,TO_DATE(BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500/01/01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date
    FROM source_ff_tss_cot_technologies_d_wtcalc
),

final AS (
    SELECT
        bl_cot_technology_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        tech_active_flag,
        sub_tech_active_flag,
        bl_creation_date,
        bl_last_update_date
    FROM transformed_expr
)

SELECT * FROM final