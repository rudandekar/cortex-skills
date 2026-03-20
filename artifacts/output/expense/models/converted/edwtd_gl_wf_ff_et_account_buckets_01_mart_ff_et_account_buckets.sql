{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_et_account_buckets', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_ET_ACCOUNT_BUCKETS',
        'target_table': 'FF_ET_ACCOUNT_BUCKETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.993667+00:00'
    }
) }}

WITH 

source_et_account_buckets AS (
    SELECT
        account_bucket_id,
        description,
        account_sequence,
        discretionary,
        controllable,
        create_date,
        create_user,
        update_date,
        update_user
    FROM {{ source('raw', 'et_account_buckets') }}
),

transformed_exp_et_account_buckets AS (
    SELECT
    account_bucket_id,
    description,
    account_sequence,
    discretionary,
    controllable,
    create_date,
    create_user,
    update_date,
    update_user,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_et_account_buckets
),

final AS (
    SELECT
        batch_id,
        account_bucket_id,
        description,
        account_sequence,
        discretionary,
        controllable,
        create_date,
        create_user,
        update_date,
        update_user,
        create_datetime,
        action_code
    FROM transformed_exp_et_account_buckets
)

SELECT * FROM final