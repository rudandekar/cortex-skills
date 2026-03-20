{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_account_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_ACCOUNT_INFO',
        'target_table': 'ST_SI_ACCOUNT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.092828+00:00'
    }
) }}

WITH 

source_ff_si_account_info AS (
    SELECT
        batch_id,
        account_value,
        account_name,
        account_type_id,
        parent_account_value,
        local_balancesheet_owner_id,
        usage_description,
        start_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_account_info') }}
),

final AS (
    SELECT
        batch_id,
        account_value,
        account_name,
        account_type_id,
        parent_account_value,
        local_balancesheet_owner_id,
        usage_description,
        start_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_account_info
)

SELECT * FROM final