{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_iam_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_SM_IAM_USER',
        'target_table': 'ST_IAM_EDWTD_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.736178+00:00'
    }
) }}

WITH 

source_ex_iam_edwtd_user AS (
    SELECT
        iam_user_id,
        universal_id,
        cco_id,
        emplid,
        cpr_access_level,
        iam_status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_iam_edwtd_user') }}
),

source_st_iam_edwtd_user AS (
    SELECT
        iam_user_id,
        universal_id,
        cco_id,
        emplid,
        cpr_access_level,
        iam_status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_user') }}
),

final AS (
    SELECT
        iam_user_id,
        universal_id,
        cco_id,
        emplid,
        cpr_access_level,
        iam_status,
        created_by,
        create_date,
        updated_by,
        update_date,
        cec_id,
        iam_user_type,
        log_level,
        action_code,
        batch_id,
        create_datetime
    FROM source_st_iam_edwtd_user
)

SELECT * FROM final