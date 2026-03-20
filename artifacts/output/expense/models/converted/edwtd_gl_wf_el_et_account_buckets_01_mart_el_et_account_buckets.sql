{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_et_account_buckets', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_ET_ACCOUNT_BUCKETS',
        'target_table': 'EL_ET_ACCOUNT_BUCKETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.831969+00:00'
    }
) }}

WITH 

source_st_et_account_buckets AS (
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
    FROM {{ source('raw', 'st_et_account_buckets') }}
),

final AS (
    SELECT
        account_bucket_id,
        description,
        account_sequence,
        discretionary,
        controllable
    FROM source_st_et_account_buckets
)

SELECT * FROM final