{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_et_account_buckets', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_ET_ACCOUNT_BUCKETS',
        'target_table': 'ST_ET_ACCOUNT_BUCKETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.081252+00:00'
    }
) }}

WITH 

source_ff_et_account_buckets AS (
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
    FROM {{ source('raw', 'ff_et_account_buckets') }}
),

final AS (
    SELECT
        batch_id,
        account_bucket_id,
        description,
        account_sequence,
        discretionary,
        controllable,
        update_date,
        create_datetime,
        action_code
    FROM source_ff_et_account_buckets
)

SELECT * FROM final