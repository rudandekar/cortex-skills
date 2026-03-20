{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sr_tech_assistance_cntr_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_SR_TECH_ASSISTANCE_CNTR_COST',
        'target_table': 'WI_SR_ENDCUST_IB_MAP_TGT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.897621+00:00'
    }
) }}

WITH 

source_wi_sr_end_cust_gl_based AS (
    SELECT
        fiscal_year_month_int,
        service_request_tac_cost_key,
        end_cust_enrichment_mthod_name,
        bk_repair_order_num_int,
        src_rptd_sr_number_int,
        src_rptd_service_contarct_num,
        c3_customer,
        dv_end_customer_name,
        sr_exp
    FROM {{ source('raw', 'wi_sr_end_cust_gl_based') }}
),

source_wi_sr_endcust_ib_map AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        dv_end_customer_name,
        sl_num,
        no_of_end_cust
    FROM {{ source('raw', 'wi_sr_endcust_ib_map') }}
),

source_n_sr_techni_assist_center_cost AS (
    SELECT
        service_request_tac_cost_key,
        src_rptd_service_request_num_int,
        bk_service_request_num_int,
        src_rptd_service_contract_num,
        c3_cust_theater_name,
        c3_cust_market_segment_name,
        src_rptd_prdt_subgroup_id,
        src_rptd_allctd_service_group_id,
        src_rptd_service_category_id,
        src_rptd_prdt_family_id,
        tac_backbone_cost_usd_amt,
        tac_theater_busi_operations_cost_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_warranty_pct,
        tac_adjusted_overhead_pct,
        src_rptd_sr_cntry_cd,
        bk_sr_tac_cost_fscl_cal_cd,
        bk_sr_tac_cost_fscl_yr_num_int,
        bk_sr_tac_cost_fscl_mth_num_int,
        bk_sr_tac_work_theater_cd,
        src_deleted_flg,
        total_actual_spent_cost_usd_amt,
        tac_non_out_tasking_cost_usd_amt,
        service_request_type_cd,
        src_rptd_cust_name,
        problem_cd,
        entry_channel_name,
        delivery_channel_name,
        actual_case_on_days_hrs_cnt,
        initial_severity_cd,
        rma_case_weight_pct,
        rma_flg,
        smart_acct_id,
        subscription_ref_id,
        tac_allctd_warranty_pct,
        techni_service_burden_cost_usd_amt,
        virtual_acct_id,
        warranty_pct,
        src_rptd_enterprise_prdt_family_name,
        src_rptd_covered_prdt_family_id,
        src_rptd_service_contract_line_name,
        src_rptd_ship_to_iso_cntry_name,
        src_rptd_hw_prdt_family_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        tac_warranty_credit_m1_usd_amt,
        tac_warranty_credit_m2_usd_amt,
        tac_warranty_credit_m3_usd_amt,
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
    FROM {{ source('raw', 'n_sr_techni_assist_center_cost') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        dv_end_customer_name,
        sl_num,
        no_of_end_cust
    FROM source_n_sr_techni_assist_center_cost
)

SELECT * FROM final