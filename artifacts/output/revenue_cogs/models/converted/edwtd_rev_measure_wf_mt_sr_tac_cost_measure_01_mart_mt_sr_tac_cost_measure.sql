{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sr_tac_cost_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_SR_TAC_COST_MEASURE',
        'target_table': 'MT_SR_TAC_COST_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.018518+00:00'
    }
) }}

WITH 

source_w_sr_tac_cost_measure AS (
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
    FROM {{ source('raw', 'w_sr_tac_cost_measure') }}
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
        edw_update_user
    FROM source_w_sr_tac_cost_measure
)

SELECT * FROM final