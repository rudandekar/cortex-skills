{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_tss_srvc_sprt_ctr_cgs_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_TSS_SRVC_SPRT_CTR_CGS_TRGLTN',
        'target_table': 'WI_RO_KY_TS_SRVC_SPT_CTR_CG_TN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.898161+00:00'
    }
) }}

WITH 

source_wi_ro_ky_ts_srvc_spt_ctr_cg_tn AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_ky_ts_srvc_spt_ctr_cg_tn') }}
),

source_w_tss_srvc_sprt_ctr_cgs_trgltn AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rprtd_srvc_contract_num,
        service_product_key,
        goods_product_key,
        bk_triangulation_type_cd,
        bk_c3_customer_theater_name,
        bk_c3_cust_market_segment_name,
        ssc_dpt_psnl_rsrc_cost_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_trdprty_lgsts_cost_usd_amt,
        ssc_trdprty_mntnc_cost_usd_amt,
        ssc_orig_eqpt_mfg_cost_usd_amt,
        ssc_warranty_pct,
        service_contract_num,
        bk_st_enrchmnt_mthdlgy_cd,
        bk_fiscal_year_mth_number_int,
        service_request_ssc_cost_key,
        shipped_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_tss_srvc_sprt_ctr_cgs_trgltn') }}
),

final AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM source_w_tss_srvc_sprt_ctr_cgs_trgltn
)

SELECT * FROM final