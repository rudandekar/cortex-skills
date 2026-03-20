{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_edw_fdi_rec_rev_trx_ff', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_EDW_FDI_REC_REV_TRX_FF',
        'target_table': 'ST_FIN_EDW_FDI_REC_REV_TRX_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.674448+00:00'
    }
) }}

WITH 

source_ff_fin_vw_edw_fdi_rec_rev_trx_n AS (
    SELECT
        fiscal_month_id,
        source_entity_name,
        adjustment_type_code,
        financial_company_code,
        product_id,
        service_product_flag,
        sales_territory_name_code,
        theater,
        gl_account_number,
        account_class,
        corporate_revenue_flag,
        extended_quantity,
        functional_currency_code,
        gl_posted_date,
        invoice_currency_code,
        transaction_date,
        usd_extended_base_price,
        usd_extended_gross_base_price,
        usd_extended_gross_cost,
        usd_extended_gross_revenue,
        usd_extended_net_base_price,
        usd_extended_net_cost,
        usd_extended_net_revenue,
        usd_extended_selling_price,
        usd_extended_standard_cost,
        usd_extended_standard_price,
        revenue_classification,
        recurring_flag,
        rev_clasf_rule_name,
        data_source_name
    FROM {{ source('raw', 'ff_fin_vw_edw_fdi_rec_rev_trx_n') }}
),

final AS (
    SELECT
        fiscal_month_id,
        source_entity_name,
        adjustment_type_code,
        financial_company_code,
        product_id,
        service_product_flag,
        sales_territory_name_code,
        theater,
        gl_account_number,
        account_class,
        corporate_revenue_flag,
        extended_quantity,
        functional_currency_code,
        gl_posted_date,
        invoice_currency_code,
        transaction_date,
        usd_extended_base_price,
        usd_extended_gross_base_price,
        usd_extended_gross_cost,
        usd_extended_gross_revenue,
        usd_extended_net_base_price,
        usd_extended_net_cost,
        usd_extended_net_revenue,
        usd_extended_selling_price,
        usd_extended_standard_cost,
        usd_extended_standard_price,
        revenue_classification,
        recurring_flag,
        rev_clasf_rule_name,
        data_source_name
    FROM source_ff_fin_vw_edw_fdi_rec_rev_trx_n
)

SELECT * FROM final