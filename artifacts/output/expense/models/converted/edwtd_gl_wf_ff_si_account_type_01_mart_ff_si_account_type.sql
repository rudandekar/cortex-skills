{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_account_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_ACCOUNT_TYPE',
        'target_table': 'FF_SI_ACCOUNT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.146780+00:00'
    }
) }}

WITH 

source_si_account_type AS (
    SELECT
        account_type_id,
        account_type_desc,
        create_date,
        created_by,
        last_update_date,
        last_update_by,
        enabled_flag
    FROM {{ source('raw', 'si_account_type') }}
),

transformed_exp_si_account_type AS (
    SELECT
    account_type_id,
    account_type_desc,
    create_date,
    created_by,
    last_update_date,
    last_update_by,
    enabled_flag,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS creare_datetime,
    'I' AS action_code
    FROM source_si_account_type
),

final AS (
    SELECT
        batch_id,
        account_type_id,
        account_type_desc,
        last_update_date,
        enabled_flag,
        create_datetime,
        action_code
    FROM transformed_exp_si_account_type
)

SELECT * FROM final