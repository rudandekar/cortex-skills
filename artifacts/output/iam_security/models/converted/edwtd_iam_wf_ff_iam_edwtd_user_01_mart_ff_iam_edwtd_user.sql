{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USER',
        'target_table': 'FF_IAM_EDWTD_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.774460+00:00'
    }
) }}

WITH 

source_iam_edwtd_user_vw AS (
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
        log_level
    FROM {{ source('raw', 'iam_edwtd_user_vw') }}
),

transformed_exp_ff_iam_edwtd_user AS (
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
    'I' AS action_code,
    ''BatchId'' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_user_vw
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
    FROM transformed_exp_ff_iam_edwtd_user
)

SELECT * FROM final