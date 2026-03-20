{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_hfm_account_balance_dfa', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_WK_HFM_ACCOUNT_BALANCE_DFA',
        'target_table': 'W_HFM_ACCOUNT_BALANCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.711623+00:00'
    }
) }}

WITH 

source_st_n_hfm_account_balance_dfa AS (
    SELECT
        bk_finance_report_line_item_cd,
        bk_view_level_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_rollup_category_cd,
        bk_intercompany_company_cd,
        bk_company_cd,
        bk_financial_account_cd,
        bk_financial_location_cd,
        bk_hfm_reported_department_cd,
        department_type,
        ru_bk_hfm_department_cd,
        ru_bk_department_code,
        consolidated_balance_usd_amt,
        fin_update_dtm
    FROM {{ source('raw', 'st_n_hfm_account_balance_dfa') }}
),

final AS (
    SELECT
        bk_financial_account_cd,
        bk_financial_location_cd,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_company_cd,
        bk_intercompany_company_cd,
        bk_finance_report_line_item_cd,
        bk_hfm_reported_department_cd,
        bk_rollup_category_cd,
        bk_view_level_cd,
        department_type,
        consolidated_balance_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_bk_hfm_department_cd,
        ru_bk_department_code,
        source_deleted_flg,
        action_code,
        dml_type
    FROM source_st_n_hfm_account_balance_dfa
)

SELECT * FROM final