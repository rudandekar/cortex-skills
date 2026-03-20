{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_tac_cost_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SR_TAC_COST_MEASURE',
        'target_table': 'W_SR_TAC_COST_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.545561+00:00'
    }
) }}

WITH 

source_n_sr_tech_assistance_cntr_cost AS (
    SELECT
        service_request_tac_cost_key,
        bk_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        src_rptd_sr_number_int,
        src_rptd_service_contarct_num,
        sr_product_subgroup_id,
        sr_allocated_service_group_id,
        sr_service_category_id,
        sr_product_family_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        source_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_biz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_warranty_pct,
        tac_adjusted_overhead_pct,
        fiscal_year_month_int,
        source_deleted_flg,
        delivery_channel_name,
        actual_case_on_days_hrs_cnt,
        src_rptd_cust_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sr_tech_assistance_cntr_cost') }}
),

source_wi_ccm_tac_id_service_key AS (
    SELECT
        fiscal_quarter_name,
        service_request_tac_cost_key,
        service_product_key
    FROM {{ source('raw', 'wi_ccm_tac_id_service_key') }}
),

source_wi_ccm_ts_mth_qtr_cntl AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        ccm_fiscal_month,
        month_rank
    FROM {{ source('raw', 'wi_ccm_ts_mth_qtr_cntl') }}
),

source_wi_ccm_tac_id_sales_terr_key AS (
    SELECT
        fiscal_quarter_name,
        service_request_tac_cost_key,
        drvd_sales_territory_key,
        sa_slk_allocation_ratio,
        allocation_type
    FROM {{ source('raw', 'wi_ccm_tac_id_sales_terr_key') }}
),

source_wi_ccm_tac_id_product_key AS (
    SELECT
        fiscal_quarter_name,
        service_request_tac_cost_key,
        goods_product_key
    FROM {{ source('raw', 'wi_ccm_tac_id_product_key') }}
),

final AS (
    SELECT
        service_request_tac_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        fiscal_year_month_int,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        src_rptd_service_contarct_num,
        tac_backbone_cost_usd_amt,
        tac_thtr_biz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        dv_tac_warranty_pct,
        dv_tac_adjusted_overhead_pct,
        bk_iso_country_code,
        enrichment_methodolody_desc,
        delivery_channel_name,
        actual_case_on_days_hrs_cnt,
        src_rptd_cust_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_wi_ccm_tac_id_product_key
)

SELECT * FROM final