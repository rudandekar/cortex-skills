{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_product_manual_adjustment', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PRODUCT_MANUAL_ADJUSTMENT',
        'target_table': 'WI_PRODUCT_MANUAL_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.038588+00:00'
    }
) }}

WITH 

source_wi_product_manual_adjustment AS (
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
        dml_type,
        bk_deal_id
    FROM {{ source('raw', 'wi_product_manual_adjustment') }}
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
        adjustment_allocation_usd_amt,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        bk_deal_id
    FROM source_wi_product_manual_adjustment
)

SELECT * FROM final