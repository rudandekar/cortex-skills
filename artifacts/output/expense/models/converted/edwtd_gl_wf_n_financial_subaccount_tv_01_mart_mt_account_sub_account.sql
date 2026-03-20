{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_subaccount_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_SUBACCOUNT_TV',
        'target_table': 'MT_ACCOUNT_SUB_ACCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.040134+00:00'
    }
) }}

WITH 

source_w_financial_subaccount AS (
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
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user,
        bk_fin_acct_locality_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_subaccount') }}
),

source_n_financial_subaccount_tv AS (
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
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user,
        bk_fin_acct_locality_int
    FROM {{ source('raw', 'n_financial_subaccount_tv') }}
),

final AS (
    SELECT
        bk_financial_account_cd,
        bk_subaccount_cd,
        sub_account_name,
        subaccount_descr,
        local_subacct_owner_party_key,
        bk_parent_fin_acct_cd,
        account_group_id,
        financial_account_type,
        financial_account_descr,
        financial_acct_start_dt,
        global_account_owner_party_key,
        parent_role,
        financial_acct_display_seq_int,
        controllable_account_flg
    FROM source_n_financial_subaccount_tv
)

SELECT * FROM final