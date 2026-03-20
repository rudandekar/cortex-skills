{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_company', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_COMPANY',
        'target_table': 'W_FINANCIAL_COMPANY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.624330+00:00'
    }
) }}

WITH 

source_st_si_company_info AS (
    SELECT
        batch_id,
        company_value,
        theater_id,
        company_name,
        functional_currency_code,
        enabled_flag,
        usage_description,
        start_date,
        end_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_company_info') }}
),

source_n_iso_currency_tv AS (
    SELECT
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        iso_currency_name,
        iso_currency_enabled_flag,
        sk_currency_code,
        ss_code,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'n_iso_currency_tv') }}
),

transformed_exp_ex_w_financial_company AS (
    SELECT
    batch_id,
    company_value,
    theater_id,
    company_name,
    functional_currency_code,
    enabled_flag,
    usage_description,
    start_date,
    end_date,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM source_n_iso_currency_tv
),

transformed_exp_w_financial_company AS (
    SELECT
    bk_company_code,
    start_date_active,
    end_date_active,
    fin_company_name,
    fin_company_enabled_flag,
    fin_company_usage_description,
    fin_company_start_date,
    fin_company_end_date,
    bk_functional_currency_code,
    bk_financial_theater_name_code,
    sk_company_value_code,
    action_code,
    rank_index,
    dml_type,
    'NE' AS error_check
    FROM transformed_exp_ex_w_financial_company
),

final AS (
    SELECT
        bk_company_code,
        start_tv_date,
        end_tv_date,
        fin_company_name,
        fin_company_enabled_flag,
        fin_company_usage_description,
        fin_company_start_date,
        fin_company_end_date,
        bk_functional_currency_code,
        bk_financial_theater_name_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_company_value_code,
        buy_sell_role,
        ru_bk_operating_unit_name_cd,
        bk_iso_country_code,
        default_sales_territory_key,
        country_enablement_flg,
        action_code,
        dml_type
    FROM transformed_exp_w_financial_company
)

SELECT * FROM final