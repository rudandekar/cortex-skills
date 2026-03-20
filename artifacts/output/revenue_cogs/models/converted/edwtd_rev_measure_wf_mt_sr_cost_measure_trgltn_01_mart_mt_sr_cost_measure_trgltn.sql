{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sr_cost_measure_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_SR_COST_MEASURE_TRGLTN',
        'target_table': 'MT_SR_COST_MEASURE_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.612611+00:00'
    }
) }}

WITH 

source_mt_sr_cost_measure_trgltn AS (
    SELECT
        service_request_tac_cost_key,
        bk_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        actual_case_on_days_hrs_cnt,
        src_rptd_service_contarct_num,
        dv_sr_volume,
        dv_sr_absolute_volume,
        fiscal_year_month_int,
        sales_territory_key,
        src_rptd_cust_name,
        goods_product_key,
        dv_service_prod_subgrp_id,
        bk_st_enrchmnt_mthdlgy_cd,
        dv_tac_sr_cost_usd_amt,
        dv_tac_sr_hourly_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_end_customer_name,
        end_cust_enrichment_mthod_name
    FROM {{ source('raw', 'mt_sr_cost_measure_trgltn') }}
),

final AS (
    SELECT
        service_request_tac_cost_key,
        bk_service_request_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        actual_case_on_days_hrs_cnt,
        src_rptd_service_contarct_num,
        dv_sr_volume,
        dv_sr_absolute_volume,
        fiscal_year_month_int,
        sales_territory_key,
        src_rptd_cust_name,
        goods_product_key,
        dv_service_prod_subgrp_id,
        bk_st_enrchmnt_mthdlgy_cd,
        dv_tac_sr_cost_usd_amt,
        dv_tac_sr_hourly_cost_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_end_customer_name,
        end_cust_enrichment_mthod_name
    FROM source_mt_sr_cost_measure_trgltn
)

SELECT * FROM final