{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_local_financial_acct', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_LOCAL_FINANCIAL_ACCT',
        'target_table': 'W_LOCAL_FINANCIAL_ACCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.086945+00:00'
    }
) }}

WITH 

source_n_fin_acct_locality_tv AS (
    SELECT
        start_tv_date,
        end_tv_date,
        financial_acct_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int,
        bk_fin_acct_locality_int
    FROM {{ source('raw', 'n_fin_acct_locality_tv') }}
),

source_st_si_account_local AS (
    SELECT
        batch_id,
        account_value,
        si_flex_struct_id,
        enabled_flag,
        reclass_account_value,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_account_local') }}
),

transformed_exp_wk_local_financial_acct AS (
    SELECT
    start_tv_date,
    end_tv_date,
    bk_fin_acct_locality_int,
    ru_bk_rcls_fin_acct_lclty_int,
    bk_financial_account_code,
    ru_bk_rcls_fin_acct_code,
    fin_local_acct_enabled_flag,
    fin_local_acct_end_date,
    reclass_role,
    sk_account_value_code,
    rank_index,
    dml_type,
    action_code,
    'NE' AS error_check
    FROM source_st_si_account_local
),

transformed_exp_ex_si_account_local AS (
    SELECT
    batch_id,
    account_value,
    si_flex_struct_id,
    enabled_flag,
    reclass_account_value,
    end_date,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM transformed_exp_wk_local_financial_acct
),

final AS (
    SELECT
        start_tv_date,
        end_tv_date,
        bk_fin_acct_locality_int,
        ru_bk_rcls_fin_acct_lclty_int,
        bk_financial_account_code,
        ru_bk_rcls_fin_acct_code,
        fin_local_acct_enabled_flag,
        fin_local_acct_end_date,
        reclass_role,
        sk_account_value_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_ex_si_account_local
)

SELECT * FROM final