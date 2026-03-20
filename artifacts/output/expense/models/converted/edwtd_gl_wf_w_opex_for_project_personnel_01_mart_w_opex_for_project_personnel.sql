{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_opex_for_project_personnel', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_OPEX_FOR_PROJECT_PERSONNEL',
        'target_table': 'W_OPEX_FOR_PROJECT_PERSONNEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.831169+00:00'
    }
) }}

WITH 

source_w_opex_for_project_personnel AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_department_cd,
        bk_financial_account_cd,
        bk_financial_location_cd,
        bk_project_cd,
        bk_subaccount_cd,
        product_key,
        bk_company_cd,
        bk_clarity_project_type_cd,
        bk_clarity_project_num,
        bk_ss_clarity_tracked_proj_cd,
        bk_allocated_flg,
        platform_name,
        opex_functional_amt,
        opex_usd_amt,
        dv_bk_fiscal_year_month_int,
        fte_cnt,
        bk_project_locality_int,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        functional_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_opex_for_project_personnel') }}
),

final AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_department_cd,
        bk_financial_account_cd,
        bk_financial_location_cd,
        bk_project_code,
        bk_subaccount_cd,
        product_key,
        bk_company_cd,
        bk_clarity_project_type_cd,
        bk_clarity_project_num,
        bk_ss_clarity_tracked_proj_cd,
        allocated_flg,
        platform_name,
        opex_functional_amt,
        opex_usd_amt,
        dv_bk_fiscal_year_month_int,
        fte_cnt,
        bk_project_locality_int,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        functional_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_opex_for_project_personnel
)

SELECT * FROM final