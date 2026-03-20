{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_capital_future_fin_dtl_acct', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_WK_CAPITAL_FUTURE_FIN_DTL_ACCT',
        'target_table': 'W_CAPITAL_FUTURE_FIN_DTL_ACCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.059503+00:00'
    }
) }}

WITH 

source_st_cap_accounts AS (
    SELECT
        batch_id,
        depreciable_life,
        read_only,
        description,
        account_type,
        comments,
        gl_account_map,
        parent_acct_id,
        account_id,
        ca,
        it,
        wpr,
        mfg,
        cdo,
        sales,
        cap_account_id,
        dep_account_id,
        line_account,
        ui_tab,
        tab_display_flag,
        cip_flag,
        rollup_flag,
        catagory,
        upload_flag,
        fi,
        cm_flag,
        hr,
        pl_account,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cap_accounts') }}
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
        account_type_cd,
        action_code,
        dml_type
    FROM source_st_cap_accounts
)

SELECT * FROM final