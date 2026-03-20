{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_tac_centers_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_TAC_CENTERS_D',
        'target_table': 'ST_TSS_TAC_CENTERS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.239878+00:00'
    }
) }}

WITH 

source_ff_tss_tac_centers_d AS (
    SELECT
        bl_tac_center_key,
        tac_center_id,
        tac_center_name,
        nlp_reporting_flag,
        enabled_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_last_update_date
    FROM {{ source('raw', 'ff_tss_tac_centers_d') }}
),

transformed_ex_st_tss_tac_centers_d AS (
    SELECT
    bl_tac_center_key,
    tac_center_id,
    tac_center_name,
    nlp_reporting_flag,
    enabled_flag,
    bl_effective_from_date,
    bl_effective_to_date,
    bl_last_update_date,
    IFF(LTRIM(RTRIM(BL_TAC_CENTER_KEY)) = '\N',-999,TO_INTEGER(BL_TAC_CENTER_KEY)) AS o_bl_tac_center_key,
    IFF(LTRIM(RTRIM(TAC_CENTER_ID)) = '\N',-999,TO_INTEGER(TAC_CENTER_ID)) AS o_tac_center_id,
    IFF(LTRIM(RTRIM(TAC_CENTER_NAME)) = '\N',NULL,TAC_CENTER_NAME) AS o_tac_center_name,
    IFF(LTRIM(RTRIM(NLP_REPORTING_FLAG)) = '\N',NULL,NLP_REPORTING_FLAG) AS o_nlp_reporting_flag,
    IFF(LTRIM(RTRIM(ENABLED_FLAG)) = '\N',NULL,ENABLED_FLAG) AS o_enabled_flag,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_FROM_DATE)) = '\N',NULL,TO_DATE(BL_EFFECTIVE_FROM_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_from_date,
    IFF(LTRIM(RTRIM(BL_EFFECTIVE_TO_DATE)) = '\N',NULL,TO_DATE(BL_EFFECTIVE_TO_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_effective_to_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date
    FROM source_ff_tss_tac_centers_d
),

final AS (
    SELECT
        bl_tac_center_key,
        tac_center_id,
        tac_center_name,
        nlp_reporting_flag,
        enabled_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_last_update_date
    FROM transformed_ex_st_tss_tac_centers_d
)

SELECT * FROM final