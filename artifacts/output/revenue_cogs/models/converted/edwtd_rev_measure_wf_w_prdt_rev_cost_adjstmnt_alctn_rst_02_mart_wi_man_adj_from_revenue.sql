{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_prdt_rev_cost_adjstmnt_alctn_rst', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_PRDT_REV_COST_ADJSTMNT_ALCTN_RST',
        'target_table': 'WI_MAN_ADJ_FROM_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.783313+00:00'
    }
) }}

WITH 

source_wi_man_adj_from_revenue AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        product_key,
        sales_territory_key,
        shipped_revenue_amt,
        shipped_cost_amt,
        e_o_amt,
        overhead_amt,
        v_o_amt,
        direct_warranty_amt
    FROM {{ source('raw', 'wi_man_adj_from_revenue') }}
),

source_wi_man_adj_from_revenue_bm AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        product_key,
        sales_territory_key,
        bk_adjustment_measure_type_cd,
        adjustment_amt
    FROM {{ source('raw', 'wi_man_adj_from_revenue_bm') }}
),

source_wi_prdt_rev_cst_adj_alctn AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        sales_territory_key,
        product_key,
        bk_adjustment_measure_type_cd,
        bk_adj_sub_measure_type_cd,
        bk_adjustment_type_cd,
        bk_adjustment_company_name,
        bk_rprtd_adjstmnt_cntry_name,
        bk_adjstmnt_sls_subcoverge_cd,
        adjustment_allocation_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'wi_prdt_rev_cst_adj_alctn') }}
),

source_wi_prdt_rev_cost_adj_alctn_rst AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        sales_territory_key,
        product_key,
        bk_adjustment_measure_type_cd,
        bk_adj_sub_measure_type_cd,
        bk_adjustment_type_cd,
        bk_adjustment_company_name,
        bk_rprtd_adjstmnt_cntry_name,
        bk_adjstmnt_sls_subcoverge_cd,
        adjustment_allocation_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'wi_prdt_rev_cost_adj_alctn_rst') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        product_key,
        sales_territory_key,
        shipped_revenue_amt,
        shipped_cost_amt,
        e_o_amt,
        overhead_amt,
        v_o_amt,
        direct_warranty_amt
    FROM source_wi_prdt_rev_cost_adj_alctn_rst
)

SELECT * FROM final