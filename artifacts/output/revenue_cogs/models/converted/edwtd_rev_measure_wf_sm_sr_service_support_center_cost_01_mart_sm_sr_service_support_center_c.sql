{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sr_service_support_center_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_SM_SR_SERVICE_SUPPORT_CENTER_COST',
        'target_table': 'SM_SR_SERVICE_SUPPORT_CENTER_C',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.823781+00:00'
    }
) }}

WITH 

source_wi_cst_ssc_cogs_fin AS (
    SELECT
        quarter,
        fiscal_month_id,
        fiscal_month,
        order_number,
        product_part_number,
        sr_number,
        order_sa_number,
        site_theater,
        site_country,
        srvc_line,
        c3_market_segment,
        srvc_offering,
        srvc_category,
        c3_cust_theater,
        hw_family,
        hybrid_product_family,
        failure_code,
        warranty_parts_pct,
        contracted_service_group,
        quantity_shipped,
        country_code,
        customer,
        lookup_theater,
        ent_product_id,
        covered_product_family,
        ent_product_family,
        ship_fiscal_month,
        extended_standard_cost,
        allocated_cost,
        dept_cost,
        depreciation_cost,
        repair_cost,
        duty_vat_cost,
        tpl_cost,
        tpm_cost,
        oem_cost,
        dept_meraki_credit_usd_amt,
        dprctn_meraki_credit_usd_amt,
        dprctn_eco_credit_usd_amt,
        repair_eco_credit_usd_amt,
        duty_vat_eco_credit_usd_amt,
        duty_vat_meraki_credit_usd_amt,
        tpl_eco_credit_usd_amt,
        tpl_meraki_credit_usd_amt,
        oem_meraki_credit_usd_amt,
        rma_credit_applied_flg,
        depr_umpire_credit_usd_amt,
        repair_umpire_credit_usd_amt,
        duty_vat_umpire_credit_usd_amt,
        tpl_umpire_credit_usd_amt
    FROM {{ source('raw', 'wi_cst_ssc_cogs_fin') }}
),

source_sm_sr_service_support_center_c AS (
    SELECT
        service_request_ssc_cost_key,
        bk_sr_ssc_cost_fscl_yr_num_int,
        bk_sr_ssc_cost_fscl_mth_num_in,
        src_rptd_service_request_num_i,
        src_rptd_repair_order_num_int,
        src_rptd_prdt_part_num,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sr_service_support_center_c') }}
),

final AS (
    SELECT
        service_request_ssc_cost_key,
        bk_sr_ssc_cost_fscl_yr_num_int,
        bk_sr_ssc_cost_fscl_mth_num_int,
        bk_shipped_fiscal_year_num_int,
        bk_shipped_fiscal_mth_num_int,
        src_rptd_service_request_num_int,
        src_rptd_repair_order_num_int,
        src_rptd_prdt_part_num,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_sr_service_support_center_c
)

SELECT * FROM final