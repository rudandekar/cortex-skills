{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_smc_1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_SMC_1',
        'target_table': 'WI_SUMMARY_QUOTE_SMC_1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.292587+00:00'
    }
) }}

WITH 

source_wi_summary_quote_smc_1 AS (
    SELECT
        sol_bk_so_number_int,
        sol_sales_order_line_key,
        sol_sales_order_key,
        bk_service_quote_num,
        quote_summary_quote_flg,
        bkg_contract_number,
        bkgs_svc_lvl,
        summary_quote_case_num,
        ru_cisco_booked_datetime,
        fiscal_quarter_name,
        fiscal_year_number_int
    FROM {{ source('raw', 'wi_summary_quote_smc_1') }}
),

final AS (
    SELECT
        sol_bk_so_number_int,
        sol_sales_order_line_key,
        sol_sales_order_key,
        bk_service_quote_num,
        quote_summary_quote_flg,
        bkg_contract_number,
        bkgs_svc_lvl,
        summary_quote_case_num,
        ru_cisco_booked_datetime,
        fiscal_quarter_name,
        fiscal_year_number_int
    FROM source_wi_summary_quote_smc_1
)

SELECT * FROM final