{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_subaccount', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_SUBACCOUNT',
        'target_table': 'EX_SI_SUB_ACCOUNT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.832730+00:00'
    }
) }}

WITH 

source_st_si_sub_account_info AS (
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
    FROM {{ source('raw', 'st_si_sub_account_info') }}
),

transformed_exp_ex_w_financial_subaccount AS (
    SELECT
    batch_id,
    bk_subaccount_code,
    bk_subaccount_locality_int,
    bk_fin_acct_locality_int,
    bk_financial_account_code,
    sub_account_name,
    subaccount_description,
    local_subacct_owner_party_key,
    sk_account_value_code,
    sk_si_flex_struct_id_int,
    sk_sub_account_value_code,
    start_tv_date,
    enabled_flag,
    edw_create_datetime,
    action_code,
    exception_type
    FROM source_st_si_sub_account_info
),

transformed_exp_w_financial_subaccount AS (
    SELECT
    bk_subaccount_code,
    subaccount_name,
    subaccount_description,
    local_subacct_owner_party_key,
    start_tv_date,
    end_tv_date,
    sk_account_value_code,
    sk_si_flex_struct_id_int,
    sk_sub_account_value_code,
    bk_subaccount_locality_int,
    bk_financial_account_code,
    bk_fin_acct_locality_int,
    action_code,
    dml_type,
    rank_index
    FROM transformed_exp_ex_w_financial_subaccount
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
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_w_financial_subaccount
)

SELECT * FROM final