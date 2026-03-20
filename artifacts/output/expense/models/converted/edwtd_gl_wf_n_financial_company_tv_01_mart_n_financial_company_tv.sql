{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_company_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_COMPANY_TV',
        'target_table': 'N_FINANCIAL_COMPANY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.951549+00:00'
    }
) }}

WITH 

source_w_financial_company AS (
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
    FROM {{ source('raw', 'w_financial_company') }}
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
        country_enablement_flg
    FROM source_w_financial_company
)

SELECT * FROM final