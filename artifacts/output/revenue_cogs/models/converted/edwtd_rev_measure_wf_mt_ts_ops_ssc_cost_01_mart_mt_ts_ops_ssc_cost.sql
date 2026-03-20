{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_ts_ops_ssc_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_TS_OPS_SSC_COST',
        'target_table': 'MT_TS_OPS_SSC_COST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.708689+00:00'
    }
) }}

WITH 

source_mt_ts_ops_ssc_cost AS (
    SELECT
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        src_rptd_repair_order_num_int,
        src_rptd_product_part_num,
        sr_product_family_id,
        dv_goods_product_key,
        service_product_key,
        src_rptd_service_contarct_num,
        dv_sales_territory_key,
        ship_to_customer_account_key,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        ssc_warranty_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_ts_ops_ssc_cost') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        src_rptd_repair_order_num_int,
        src_rptd_product_part_num,
        sr_product_family_id,
        dv_goods_product_key,
        service_product_key,
        src_rptd_service_contarct_num,
        dv_sales_territory_key,
        ship_to_customer_account_key,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        ssc_warranty_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_ts_ops_ssc_cost
)

SELECT * FROM final