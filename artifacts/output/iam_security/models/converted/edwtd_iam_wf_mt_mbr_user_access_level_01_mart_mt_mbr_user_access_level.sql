{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_mbr_user_access_level', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_MBR_USER_ACCESS_LEVEL',
        'target_table': 'MT_MBR_USER_ACCESS_LEVEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.752244+00:00'
    }
) }}

WITH 

source_mt_mbr_user_access_level AS (
    SELECT
        bk_cec_id,
        top_level_int,
        subsequent_level_int,
        bk_application_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_mbr_user_access_level') }}
),

final AS (
    SELECT
        bk_cec_id,
        top_level_int,
        subsequent_level_int,
        bk_application_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_mbr_user_access_level
)

SELECT * FROM final