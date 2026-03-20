{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rma_cost_measure_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_RMA_COST_MEASURE_TRGLTN',
        'target_table': 'MT_RMA_COST_MEASURE_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.830229+00:00'
    }
) }}

WITH 

source_mt_rma_cost_measure_trgltn AS (
    SELECT
        service_request_ssc_cost_key,
        bk_service_request_num_int,
        src_rptd_repair_order_num_int,
        contracted_service_grp_name,
        shipped_qty,
        src_rptd_service_contarct_num,
        fiscal_year_month_int,
        sales_territory_key,
        dv_rma_volume,
        dv_rma_absolute_volume,
        src_rptd_site_cntry_name,
        src_rptd_cust_name,
        goods_product_key,
        dv_service_prod_subgrp_id,
        bk_st_enrchmnt_mthdlgy_cd,
        ssc_dpt_psnl_rsrc_cost_usd_amt,
        ssc_trdprty_mntnc_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_orig_eqpt_mfg_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        dv_ssc_rma_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_end_customer_name,
        end_cust_enrichment_mthod_name
    FROM {{ source('raw', 'mt_rma_cost_measure_trgltn') }}
),

final AS (
    SELECT
        service_request_ssc_cost_key,
        bk_service_request_num_int,
        src_rptd_repair_order_num_int,
        contracted_service_grp_name,
        shipped_qty,
        src_rptd_service_contarct_num,
        fiscal_year_month_int,
        sales_territory_key,
        dv_rma_volume,
        dv_rma_absolute_volume,
        src_rptd_site_cntry_name,
        src_rptd_cust_name,
        goods_product_key,
        dv_service_prod_subgrp_id,
        bk_st_enrchmnt_mthdlgy_cd,
        ssc_dpt_psnl_rsrc_cost_usd_amt,
        ssc_trdprty_mntnc_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_orig_eqpt_mfg_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        dv_ssc_rma_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_end_customer_name,
        end_cust_enrichment_mthod_name
    FROM source_mt_rma_cost_measure_trgltn
)

SELECT * FROM final