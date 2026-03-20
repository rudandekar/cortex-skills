{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_msr_corpadj_get', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_MSR_CORPADJ_GET',
        'target_table': 'WI_GL_REV_MSR_TRNGLTD_CORPGET',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.087858+00:00'
    }
) }}

WITH 

source_wi_gl_rev_msr_corpadj_sl1 AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        l1_sales_territory_descr,
        sales_territory_key,
        dv_comp_us_net_rev_amt,
        rev_amt_by_l1,
        ratio_by_sl_key
    FROM {{ source('raw', 'wi_gl_rev_msr_corpadj_sl1') }}
),

source_wi_gl_rev_msr_sales_hier AS (
    SELECT
        dv_fiscal_month_to_year_key,
        sales_territory_key,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        sales_territory_type_code,
        l1_sales_territory_descr
    FROM {{ source('raw', 'wi_gl_rev_msr_sales_hier') }}
),

source_wi_gl_rev_measure_icpm AS (
    SELECT
        product_key,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        dv_credit_memo_amt,
        dv_debit_memo_amt,
        dv_inv_rev_base_list_amt,
        dv_shipped_rev_amt,
        dv_net_adj_amt,
        dv_rev_standard_cost_amt,
        dv_direct_rev_adj_amt,
        dv_direct_cost_adj_amt,
        dv_indirect_rev_adj_amt,
        dv_indirect_cogs_adj_amt,
        dv_gmb_cogs_adj_amt,
        dv_excess_obsolete_adj_amt,
        dv_overhead_adj_amt,
        dv_variance_adj_amt,
        dv_warranty_adj_amt
    FROM {{ source('raw', 'wi_gl_rev_measure_icpm') }}
),

source_wi_gl_rev_msr_corp_get AS (
    SELECT
        l2_sales_territory_name_code,
        product_key,
        sales_territory_key,
        iso_country_code,
        dv_fiscal_year_mth_number_int,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        dv_credit_memo_amt,
        dv_debit_memo_amt,
        dv_inv_rev_base_list_amt,
        dv_shipped_rev_amt,
        dv_net_adj_amt,
        dv_rev_standard_cost_amt,
        dv_direct_rev_adj_amt,
        dv_direct_cost_adj_amt,
        dv_indirect_rev_adj_amt,
        dv_indirect_cogs_adj_amt,
        dv_gmb_cogs_adj_amt,
        dv_excess_obsolete_adj_amt,
        dv_overhead_adj_amt,
        dv_variance_adj_amt,
        dv_warranty_adj_amt
    FROM {{ source('raw', 'wi_gl_rev_msr_corp_get') }}
),

final AS (
    SELECT
        triangulation_type_id_int,
        product_key,
        sales_territory_key,
        drvd_sales_territory_key,
        dv_fiscal_year_mth_number_int,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        dv_credit_memo_amt,
        dv_debit_memo_amt,
        dv_inv_rev_base_list_amt,
        dv_shipped_rev_amt,
        dv_net_adj_amt,
        dv_rev_standard_cost_amt,
        dv_direct_rev_adj_amt,
        dv_direct_cost_adj_amt,
        dv_indirect_rev_adj_amt,
        dv_indirect_cogs_adj_amt,
        dv_gmb_cogs_adj_amt,
        dv_excess_obsolete_adj_amt,
        dv_overhead_adj_amt,
        dv_variance_adj_amt,
        dv_warranty_adj_amt
    FROM source_wi_gl_rev_msr_corp_get
)

SELECT * FROM final