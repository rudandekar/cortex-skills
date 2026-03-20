{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sr_srvc_support_center_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_SR_SRVC_SUPPORT_CENTER_COST',
        'target_table': 'WI_ENDCUST_REV_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.290331+00:00'
    }
) }}

WITH 

source_wi_rma_endcust_ib_map AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        bk_repair_order_num_int,
        dv_end_customer_name,
        sl_num,
        no_of_end_cust
    FROM {{ source('raw', 'wi_rma_endcust_ib_map') }}
),

source_wi_endcust_rev_map AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        service_contract,
        dv_end_customer_name,
        dv_comp_us_net_rev_amt,
        total_comp_us_net_rev_amt,
        slk_dist_count
    FROM {{ source('raw', 'wi_endcust_rev_map') }}
),

source_wi_rma_end_cust_gl_based AS (
    SELECT
        fiscal_year_month_int,
        service_request_ssc_cost_key,
        end_cust_enrichment_mthod_name,
        bk_repair_order_num_int,
        src_rptd_service_contarct_num,
        c3_customer,
        dv_end_customer_name,
        rma_exp
    FROM {{ source('raw', 'wi_rma_end_cust_gl_based') }}
),

source_n_sr_service_support_center_cost AS (
    SELECT
        service_request_ssc_cost_key,
        src_rptd_service_request_num_int,
        bk_service_request_num_int,
        src_rptd_service_contract_num,
        c3_cust_theater_name,
        c3_cust_market_segment_name,
        src_rptd_prdt_subgroup_id,
        src_rptd_allctd_service_group_id,
        src_rptd_service_category_id,
        src_rptd_prdt_family_id,
        ssc_dept_prsnl_rsrc_cost_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_third_prty_logistics_cost_usd_amt,
        ssc_third_party_mntnc_cost_usd_amt,
        ssc_orig_equip_mnfctr_cost_usd_amt,
        ssc_warranty_pct,
        src_rptd_repair_order_num_int,
        src_rptd_prdt_part_num,
        bk_sr_ssc_cost_fscl_cal_cd,
        bk_sr_ssc_cost_fscl_yr_num_int,
        bk_sr_ssc_cost_fscl_mth_num_int,
        bk_repair_order_num_int,
        bk_sr_hw_prdt_key,
        src_deleted_flg,
        failure_cd,
        warranty_parts_pct,
        contracted_service_group_name,
        shipped_qty,
        src_rptd_cust_name,
        extnd_std_cost_usd_amt,
        allocated_cost_usd_amt,
        lookup_theater_name,
        bk_shipped_fiscal_cal_cd,
        bk_shipped_fiscal_year_num_int,
        bk_shipped_fiscal_mth_num_int,
        service_offering_name,
        install_site_theater_name,
        src_rptd_ship_to_cntry_name,
        src_rptd_enterprise_prdt_id,
        src_rptd_covered_prdt_family_name,
        src_rptd_enterprise_prdt_family_name,
        src_rptd_service_contract_line_name,
        src_rptd_hw_prdt_family_name,
        src_rptd_hybrid_prdt_family_name,
        src_rptd_site_cntry_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ssc_warranty_credit_m1_usd_amt,
        ssc_warranty_credit_m2_usd_amt,
        ssc_warranty_credit_m3_usd_amt,
        bk_wc_m1_fiscal_cal_cd,
        bk_wc_m1_fiscal_year_num_int,
        bk_wc_m1_fiscal_mth_num_int,
        bk_wc_m2_fiscal_cal_cd,
        bk_wc_m2_fiscal_year_num_int,
        bk_wc_m2_fiscal_mth_num_int,
        bk_wc_m3_fiscal_cal_cd,
        bk_wc_m3_fiscal_year_num_int,
        bk_wc_m3_fiscal_mth_num_int,
        dv_end_customer_name,
        end_cust_enrichment_mthod_name
    FROM {{ source('raw', 'n_sr_service_support_center_cost') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        service_contract,
        dv_end_customer_name,
        dv_comp_us_net_rev_amt,
        total_comp_us_net_rev_amt,
        slk_dist_count
    FROM source_n_sr_service_support_center_cost
)

SELECT * FROM final