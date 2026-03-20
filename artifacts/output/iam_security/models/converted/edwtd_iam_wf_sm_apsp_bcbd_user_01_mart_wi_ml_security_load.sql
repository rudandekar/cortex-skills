{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_apsp_bcbd_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_SM_APSP_BCBD_USER',
        'target_table': 'WI_ML_SECURITY_LOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.761787+00:00'
    }
) }}

WITH 

source_wi_ml_security_load AS (
    SELECT
        user_id,
        role_name
    FROM {{ source('raw', 'wi_ml_security_load') }}
),

source_sm_apsp_bcbd_user AS (
    SELECT
        bcbd_user_key,
        iam_user_key,
        bk_bcbd_mailer_alias_addr,
        bk_bcbd_user_type,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_apsp_bcbd_user') }}
),

final AS (
    SELECT
        user_id,
        role_name
    FROM source_sm_apsp_bcbd_user
)

SELECT * FROM final