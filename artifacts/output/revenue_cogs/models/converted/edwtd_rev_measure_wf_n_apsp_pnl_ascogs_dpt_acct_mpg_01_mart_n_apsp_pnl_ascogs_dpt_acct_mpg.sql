{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_pnl_ascogs_dpt_acct_mpg', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_APSP_PNL_ASCOGS_DPT_ACCT_MPG',
        'target_table': 'N_APSP_PNL_ASCOGS_DPT_ACCT_MPG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.084835+00:00'
    }
) }}

WITH 

source_w_apsp_pnl_ascogs_dpt_acct_mpg AS (
    SELECT
        bk_department_cd,
        bk_company_cd,
        bk_pnl_measure_name,
        bk_fiscal_year_month_num_int,
        bk_financial_account_cd,
        financial_acct_reporting_flg,
        allocation_method_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_pnl_ascogs_dpt_acct_mpg') }}
),

final AS (
    SELECT
        bk_department_cd,
        bk_company_code,
        bk_pnl_measure_name,
        bk_fiscal_year_month_num_int,
        bk_financial_account_cd,
        financial_acct_reporting_flg,
        allocation_method_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_apsp_pnl_ascogs_dpt_acct_mpg
)

SELECT * FROM final