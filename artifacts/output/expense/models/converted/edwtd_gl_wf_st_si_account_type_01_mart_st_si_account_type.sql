{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_account_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_ACCOUNT_TYPE',
        'target_table': 'ST_SI_ACCOUNT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.160226+00:00'
    }
) }}

WITH 

source_ff_si_account_type AS (
    SELECT
        batch_id,
        account_type_id,
        account_type_desc,
        last_update_date,
        enabled_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_account_type') }}
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
    FROM source_ff_si_account_type
)

SELECT * FROM final