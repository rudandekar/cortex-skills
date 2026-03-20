{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_inv_rev_chnl_msr_dvad_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_INV_REV_CHNL_MSR_DVAD_MTH',
        'target_table': 'WI_INV_REV_CHNL_DAVD_PSPK_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.377816+00:00'
    }
) }}

WITH 

source_wi_inv_rev_channel_measure_mth AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        rtm_type,
        ar_trx_key,
        dv_drct_val_added_dsti_ord_flg
    FROM {{ source('raw', 'wi_inv_rev_channel_measure_mth') }}
),

source_wi_inv_rev_chnl_dvad_ptnr_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        ar_trx_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_inv_rev_chnl_dvad_ptnr_mth') }}
),

source_wi_inv_rev_chnl_dvad_ptns_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        ar_trx_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_inv_rev_chnl_dvad_ptns_mth') }}
),

source_wi_inv_rev_chnl_dvad_prt_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        ar_trx_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_inv_rev_chnl_dvad_prt_mth') }}
),

source_wi_inv_rev_chnl_davd_pspk_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        deals_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        ar_trx_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_inv_rev_chnl_davd_pspk_mth') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        deals_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        ar_trx_key,
        ship_to_customer_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM source_wi_inv_rev_chnl_davd_pspk_mth
)

SELECT * FROM final