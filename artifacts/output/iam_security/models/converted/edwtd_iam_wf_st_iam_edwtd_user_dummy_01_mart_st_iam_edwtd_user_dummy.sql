{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_user_dummy', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_USER_DUMMY',
        'target_table': 'ST_IAM_EDWTD_USER_DUMMY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.734635+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_user_dummy AS (
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
    FROM {{ source('raw', 'ff_iam_edwtd_user_dummy') }}
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
    FROM source_ff_iam_edwtd_user_dummy
)

SELECT * FROM final