{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_adv_srvcs_cogs_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_ADV_SRVCS_COGS_ALLOCATION',
        'target_table': 'W_ADV_SRVCS_COGS_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.247997+00:00'
    }
) }}

WITH 

source_w_adv_srvcs_cogs_allocation AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_financial_account_cd,
        bk_department_cd,
        bk_company_cd,
        bk_sales_territory_key,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        bk_adv_srvcs_cgs_trgln_type_cd,
        advanced_services_cogs_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        allocation_method_name,
        dv_goods_prdt_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_adv_srvcs_cogs_allocation') }}
),

final AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_financial_account_cd,
        bk_department_cd,
        bk_company_cd,
        bk_sales_territory_key,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        bk_adv_srvcs_cgs_trgln_type_cd,
        advanced_services_cogs_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        allocation_method_name,
        dv_goods_prdt_key,
        bk_as_project_cd,
        action_code,
        dml_type
    FROM source_w_adv_srvcs_cogs_allocation
)

SELECT * FROM final