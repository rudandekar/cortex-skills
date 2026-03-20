{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_general_ledger_account', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_GENERAL_LEDGER_ACCOUNT',
        'target_table': 'N_GENERAL_LEDGER_ACCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.829763+00:00'
    }
) }}

WITH 

source_w_general_ledger_account AS (
    SELECT
        general_ledger_account_key,
        bk_company_code,
        bk_department_code,
        bk_fin_acct_locality_int,
        bk_financial_account_code,
        bk_financial_location_code,
        bk_project_code,
        bk_project_locality_int,
        bk_subaccount_code,
        bk_subaccount_locality_int,
        gl_account_enabled_flag,
        gl_account_end_date,
        gl_account_start_date,
        gl_account_type_code,
        set_of_books_key,
        sk_code_combination_id_int,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_general_ledger_account') }}
),

final AS (
    SELECT
        general_ledger_account_key,
        bk_company_code,
        bk_department_code,
        bk_fin_acct_locality_int,
        bk_financial_account_code,
        bk_financial_location_code,
        bk_project_code,
        bk_project_locality_int,
        bk_subaccount_code,
        bk_subaccount_locality_int,
        gl_account_enabled_flag,
        gl_account_end_date,
        gl_account_start_date,
        gl_account_type_code,
        set_of_books_key,
        sk_code_combination_id_int,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM source_w_general_ledger_account
)

SELECT * FROM final