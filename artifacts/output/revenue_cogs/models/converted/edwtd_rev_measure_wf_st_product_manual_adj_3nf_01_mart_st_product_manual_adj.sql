{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_product_manual_adj_3nf', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_PRODUCT_MANUAL_ADJ_3NF',
        'target_table': 'ST_PRODUCT_MANUAL_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.081993+00:00'
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
        fiscal_month_id,
        company_name,
        sales_territory_key,
        country_name,
        sales_subcoverage_code,
        product_id,
        measure_name,
        sub_measure_name,
        data_type,
        amount,
        ges_update_date,
        source_type,
        deal_id
    FROM source_n_prdt_rev_cost_adjstmnt_alctn
)

SELECT * FROM final