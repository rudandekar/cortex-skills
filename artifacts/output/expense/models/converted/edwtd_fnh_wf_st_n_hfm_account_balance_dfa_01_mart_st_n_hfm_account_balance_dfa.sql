{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_hfm_account_balance_dfa', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_HFM_ACCOUNT_BALANCE_DFA',
        'target_table': 'ST_N_HFM_ACCOUNT_BALANCE_DFA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.650284+00:00'
    }
) }}

WITH 

source_ff_n_hfm_account_balance_dfa AS (
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
    FROM {{ source('raw', 'ff_n_hfm_account_balance_dfa') }}
),

final AS (
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
    FROM source_ff_n_hfm_account_balance_dfa
)

SELECT * FROM final