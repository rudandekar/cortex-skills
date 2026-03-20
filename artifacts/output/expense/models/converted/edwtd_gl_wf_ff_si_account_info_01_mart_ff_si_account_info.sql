{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_account_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_ACCOUNT_INFO',
        'target_table': 'FF_SI_ACCOUNT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.740430+00:00'
    }
) }}

WITH 

source_si_account_info AS (
    SELECT
        account_value,
        account_name,
        account_type_id,
        usage_description,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        start_date,
        is_parent_flag,
        parent_account_value,
        local_balance_sheet_owner_id,
        info_published,
        erp_published
    FROM {{ source('raw', 'si_account_info') }}
),

transformed_exp_si_account_info AS (
    SELECT
    account_value,
    account_name,
    account_type_id,
    parent_account_value,
    local_balance_sheet_owner_id,
    usage_description,
    start_date,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_account_info
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
    FROM transformed_exp_si_account_info
)

SELECT * FROM final