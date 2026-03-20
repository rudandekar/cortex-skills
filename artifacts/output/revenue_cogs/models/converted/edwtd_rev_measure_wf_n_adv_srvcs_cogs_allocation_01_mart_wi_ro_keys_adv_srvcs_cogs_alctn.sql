{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_adv_srvcs_cogs_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_ADV_SRVCS_COGS_ALLOCATION',
        'target_table': 'WI_RO_KEYS_ADV_SRVCS_COGS_ALCTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.478954+00:00'
    }
) }}

WITH 

source_wi_ro_keys_adv_srvcs_cogs_alctn AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_keys_adv_srvcs_cogs_alctn') }}
),

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
        allocation_method_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_adv_srvcs_cogs_allocation') }}
),

final AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM source_w_adv_srvcs_cogs_allocation
)

SELECT * FROM final