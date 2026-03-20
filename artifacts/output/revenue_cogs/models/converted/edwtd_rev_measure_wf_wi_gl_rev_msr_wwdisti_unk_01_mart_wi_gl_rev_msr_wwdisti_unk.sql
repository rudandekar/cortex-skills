{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_msr_wwdisti_unk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_MSR_WWDISTI_UNK',
        'target_table': 'WI_GL_REV_MSR_WWDISTI_UNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.867163+00:00'
    }
) }}

WITH 

source_wi_gl_rev_msr_wwdisti AS (
    SELECT
        l3_sales_territory_name_code,
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
    FROM {{ source('raw', 'wi_gl_rev_msr_wwdisti') }}
),

source_wi_bkgs_msr_icpm_pos_alc_unk AS (
    SELECT
        l3_sales_territory_name_code,
        l2_sales_territory_name_code,
        pos_sales_territory_key,
        sl6_comp_us_net_price_amt,
        sl3_comp_us_net_price_amt,
        pos_allocation_ratio
    FROM {{ source('raw', 'wi_bkgs_msr_icpm_pos_alc_unk') }}
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
        product_key,
        sales_territory_key,
        pos_sales_territory_key,
        dv_fiscal_year_mth_number_int,
        triangulation_type_id_int,
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
    FROM source_mt_triangulation_type
)

SELECT * FROM final