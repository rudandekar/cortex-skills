{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_prdt_rev_cost_adjstmnt_alctn_rst', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_PRDT_REV_COST_ADJSTMNT_ALCTN_RST',
        'target_table': 'N_PRDT_REV_COST_ADJSTMNT_ALCTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.253611+00:00'
    }
) }}

WITH 

source_n_prdt_rev_cost_adjstmnt_alctn AS (
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
        bk_deal_id
    FROM {{ source('raw', 'n_prdt_rev_cost_adjstmnt_alctn') }}
),

final AS (
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
        goods_product_key,
        adjustment_allocation_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_deal_id
    FROM source_n_prdt_rev_cost_adjstmnt_alctn
)

SELECT * FROM final