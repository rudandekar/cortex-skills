{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_gl_triang_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_GL_TRIANG_ALLOCATION',
        'target_table': 'ST_AE_GL_TRIANG_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.295810+00:00'
    }
) }}

WITH 

source_ff_ae_gl_triang_allocation AS (
    SELECT
        product_id,
        sales_territory_name_code,
        drvd_sales_territory_name_code,
        dv_fiscal_year_mth_number_int,
        adjustment_type_cd,
        revenue_or_cogs_type_cd,
        dd_financial_account_cd,
        dd_direct_corp_adj_type_cd,
        rev_measure_trans_type_cd,
        transacation_type_category_cd,
        dv_corporate_revenue_flg,
        dv_ic_revenue_flg,
        dv_charges_flg,
        dv_misc_flg,
        dv_service_flg,
        dv_international_demo_flg,
        dv_replacement_demo_flg,
        dv_revenue_flg,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        sub_measure_key,
        triangulation_type_id,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        pl_line_item,
        deal_id
    FROM {{ source('raw', 'ff_ae_gl_triang_allocation') }}
),

final AS (
    SELECT
        product_id,
        sales_territory_name_code,
        drvd_sales_territory_name_code,
        dv_fiscal_year_mth_number_int,
        adjustment_type_cd,
        revenue_or_cogs_type_cd,
        dd_financial_account_cd,
        dd_direct_corp_adj_type_cd,
        rev_measure_trans_type_cd,
        transacation_type_category_cd,
        dv_corporate_revenue_flg,
        dv_ic_revenue_flg,
        dv_charges_flg,
        dv_misc_flg,
        dv_service_flg,
        dv_international_demo_flg,
        dv_replacement_demo_flg,
        dv_revenue_flg,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        sub_measure_key,
        triangulation_type_id,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        pl_line_item,
        deal_id
    FROM source_ff_ae_gl_triang_allocation
)

SELECT * FROM final