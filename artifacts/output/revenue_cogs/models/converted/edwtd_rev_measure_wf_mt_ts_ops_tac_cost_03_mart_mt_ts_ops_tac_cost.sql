{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_ts_ops_tac_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_TS_OPS_TAC_COST',
        'target_table': 'MT_TS_OPS_TAC_COST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.943291+00:00'
    }
) }}

WITH 

source_wi_tss_tac_cost_fin AS (
    SELECT
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        bk_sr_tac_work_theater_cd,
        tac_adjusted_overhead_pct,
        tac_warranty_pct,
        sales_territory_key,
        goods_product_key,
        service_product_key
    FROM {{ source('raw', 'wi_tss_tac_cost_fin') }}
),

source_wi_tss_tac_cost_ts_ops AS (
    SELECT
        fiscal_year_month_int,
        src_rptd_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        src_rptd_prdt_family_id,
        src_rptd_service_contract_num,
        c3_cust_theater_name,
        c3_cust_market_segment_name,
        sr_customer_party_key,
        bb_cost,
        ot_cost,
        oh_cost,
        tbo_cost,
        rma_case_weight
    FROM {{ source('raw', 'wi_tss_tac_cost_ts_ops') }}
),

source_mt_ts_ops_tac_cost AS (
    SELECT
        dv_fiscal_year_month_int,
        src_rptd_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        service_product_key,
        dv_goods_product_key,
        src_rptd_prdt_family_id,
        dv_sales_territory_key,
        src_rptd_service_contract_num,
        sr_customer_party_key,
        c3_cust_theater_name,
        c3_cust_market_segment_name,
        rma_case_weight_pct,
        tac_backbone_cost_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_theater_busi_operations_cost_usd_amt,
        tac_adjusted_overhead_pct,
        tac_warranty_pct,
        edw_update_user,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm
    FROM {{ source('raw', 'mt_ts_ops_tac_cost') }}
),

final AS (
    SELECT
        dv_fiscal_year_month_int,
        src_rptd_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        service_product_key,
        dv_goods_product_key,
        src_rptd_prdt_family_id,
        dv_sales_territory_key,
        src_rptd_service_contract_num,
        sr_customer_party_key,
        c3_cust_theater_name,
        c3_cust_market_segment_name,
        rma_case_weight_pct,
        tac_backbone_cost_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_theater_busi_operations_cost_usd_amt,
        tac_adjusted_overhead_pct,
        tac_warranty_pct,
        edw_update_user,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm
    FROM source_mt_ts_ops_tac_cost
)

SELECT * FROM final