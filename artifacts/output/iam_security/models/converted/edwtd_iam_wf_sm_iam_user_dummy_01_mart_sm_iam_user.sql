{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_iam_user_dummy', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_SM_IAM_USER_DUMMY',
        'target_table': 'SM_IAM_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.721560+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_user_dummy AS (
    SELECT
        iam_user_id,
        universal_id,
        cco_id,
        cec_id,
        iam_user_type,
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
    FROM {{ source('raw', 'st_iam_edwtd_user_dummy') }}
),

final AS (
    SELECT
        iam_user_key,
        sk_iam_user_id_int,
        edw_create_user,
        edw_create_dtm
    FROM source_st_iam_edwtd_user_dummy
)

SELECT * FROM final