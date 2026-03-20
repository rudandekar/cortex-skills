{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_local_financial_acct_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_LOCAL_FINANCIAL_ACCT_TV',
        'target_table': 'N_LOCAL_FINANCIAL_ACCT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.708077+00:00'
    }
) }}

WITH 

source_w_local_financial_acct AS (
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
    FROM {{ source('raw', 'w_local_financial_acct') }}
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
        edw_update_user
    FROM source_w_local_financial_acct
)

SELECT * FROM final