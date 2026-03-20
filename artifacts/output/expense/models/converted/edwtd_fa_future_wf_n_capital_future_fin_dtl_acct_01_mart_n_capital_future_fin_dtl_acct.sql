{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_capital_future_fin_dtl_acct', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_N_CAPITAL_FUTURE_FIN_DTL_ACCT',
        'target_table': 'N_CAPITAL_FUTURE_FIN_DTL_ACCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.831742+00:00'
    }
) }}

WITH 

source_w_capital_future_fin_dtl_acct AS (
    SELECT
        bk_financial_account_cd,
        bk_cptl_future_fin_dtl_acct_cd,
        sk_account_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        account_type_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_capital_future_fin_dtl_acct') }}
),

final AS (
    SELECT
        bk_financial_account_cd,
        bk_cptl_future_fin_dtl_acct_cd,
        sk_account_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        account_type_cd
    FROM source_w_capital_future_fin_dtl_acct
)

SELECT * FROM final