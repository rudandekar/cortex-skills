{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_sub_account_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_SUB_ACCOUNT_INFO',
        'target_table': 'FF_SI_SUB_ACCOUNT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.984686+00:00'
    }
) }}

WITH 

source_si_sub_account_info AS (
    SELECT
        account_value,
        si_flex_struct_id,
        sub_account_value,
        sub_account_name,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        info_published,
        erp_published,
        enabled_flag,
        usage_description,
        local_balance_sheet_owner_id,
        start_date,
        end_date,
        reclass_sub_account_value
    FROM {{ source('raw', 'si_sub_account_info') }}
),

transformed_exp_si_sub_account_info AS (
    SELECT
    account_value,
    si_flex_struct_id,
    sub_account_value,
    sub_account_name,
    enabled_flag,
    usage_description,
    local_balance_sheet_owner_id,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS account_code
    FROM source_si_sub_account_info
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
        account_code
    FROM transformed_exp_si_sub_account_info
)

SELECT * FROM final