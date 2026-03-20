{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_pnl_rev_cost_msr_cntr_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_PNL_REV_COST_MSR_CNTR_COGS',
        'target_table': 'EL_PNL_REV_COST_MSR_CNTR_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.810467+00:00'
    }
) }}

WITH 

source_el_pnl_rev_cost_msr_cntr_cogs AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        pnl_line_item_name,
        ru_bk_product_family_id,
        internal_be_descr,
        internal_sub_be_descr,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        sales_coverage_cd,
        dd_external_theater_name,
        bk_iso_country_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_revenue_or_cogs_type_cd,
        dv_cx_gsp_or_cx_product,
        comp_us_net_rev_amt,
        divestiture_flg
    FROM {{ source('raw', 'el_pnl_rev_cost_msr_cntr_cogs') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        pnl_line_item_name,
        ru_bk_product_family_id,
        internal_be_descr,
        internal_sub_be_descr,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        sales_coverage_cd,
        dd_external_theater_name,
        bk_iso_country_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_revenue_or_cogs_type_cd,
        dv_cx_gsp_or_cx_product,
        comp_us_net_rev_amt,
        divestiture_flg
    FROM source_el_pnl_rev_cost_msr_cntr_cogs
)

SELECT * FROM final