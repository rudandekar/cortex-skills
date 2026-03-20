{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_msr_scms_sl3_cty_mf', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_MSR_SCMS_SL3_CTY_MF',
        'target_table': 'WI_GL_REV_MSR_SCMS_MIX_SL3',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.515760+00:00'
    }
) }}

WITH 

source_wi_gl_rev_msr_scms_other AS (
    SELECT
        l3_sales_territory_name_code,
        l1_sales_territory_name_code,
        sales_coverage_code,
        product_key,
        dv_fiscal_year_mth_number_int,
        iso_country_code,
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
    FROM {{ source('raw', 'wi_gl_rev_msr_scms_other') }}
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

source_r_sales_hierarchy AS (
    SELECT
        sales_territory_key,
        l0_sales_territory_name_code,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l4_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        l7_sales_territory_name_code,
        sales_terr_effective_date,
        sales_terr_expiration_date,
        sales_coverage_code,
        sales_subcoverage_code,
        has_country_role,
        sales_territory_node_type,
        iso_country_code,
        sales_structure_ver_name,
        sales_structure_type,
        ss_erp_version_id_int,
        sales_territory_descr,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        l4_sales_territory_descr,
        l5_sales_territory_descr,
        l6_sales_territory_descr,
        l7_sales_territory_descr,
        bk_sales_territory_name,
        sales_territory_type_code,
        l1_sales_territory_sort_descr,
        l2_sales_territory_sort_descr,
        l3_sales_territory_sort_descr,
        l4_sales_territory_sort_descr,
        l5_sales_territory_sort_descr,
        l6_sales_territory_sort_descr,
        l7_sales_territory_sort_descr,
        dv_sales_terr_level_num_int,
        dd_external_theater_name,
        dv_carry_forward_record_flg,
        sales_territory_name_code
    FROM {{ source('raw', 'r_sales_hierarchy') }}
),

source_mt_triangulation_type AS (
    SELECT
        triangulation_type_id_int,
        triangulation_type_desc,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_triangulation_type') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        revenue_mix_scms_amt,
        net_rev_mix_l3_amount,
        net_rev_mix_ratio
    FROM source_mt_triangulation_type
)

SELECT * FROM final