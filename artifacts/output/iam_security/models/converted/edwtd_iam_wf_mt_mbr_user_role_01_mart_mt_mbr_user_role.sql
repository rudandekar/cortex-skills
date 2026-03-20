{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_mbr_user_role', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_MBR_USER_ROLE',
        'target_table': 'MT_MBR_USER_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.784835+00:00'
    }
) }}

WITH 

source_mt_mbr_user_role AS (
    SELECT
        bk_cec_id,
        initialization_variable_name,
        initialization_value_txt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_mbr_user_role') }}
),

final AS (
    SELECT
        bk_cec_id,
        initialization_variable_name,
        initialization_value_txt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_mbr_user_role
)

SELECT * FROM final