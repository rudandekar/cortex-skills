{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_sub_account_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_SUB_ACCOUNT_INFO',
        'target_table': 'ST_SI_SUB_ACCOUNT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.753986+00:00'
    }
) }}

WITH 

source_ff_si_sub_account_info AS (
    SELECT
        batch_id,
        account_value,
        si_flex_struct_id,
        sub_account_value,
        sub_account_name,
        enabled_flag,
        usage_description,
        local_balance_sheet_owner_id,
        last_update_date,
        create_datetime,
        account_code
    FROM {{ source('raw', 'ff_si_sub_account_info') }}
),

final AS (
    SELECT
        batch_id,
        account_value,
        si_flex_struct_id,
        sub_account_value,
        sub_account_name,
        enabled_flag,
        usage_description,
        local_balance_sheet_owner_id,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_sub_account_info
)

SELECT * FROM final