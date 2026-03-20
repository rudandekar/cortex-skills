{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_channel_msr_dvad', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_CHANNEL_MSR_DVAD',
        'target_table': 'WI_GL_REV_CHANNEL_DVAD_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.619502+00:00'
    }
) }}

WITH 

source_wi_gl_rev_channel_dvad_patns AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        ar_trx_key,
        ship_to_customer_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_gl_rev_channel_dvad_patns') }}
),

source_wi_gl_rev_channel_davd_pspk AS (
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
    FROM {{ source('raw', 'wi_gl_rev_channel_davd_pspk') }}
),

source_wi_gl_rev_channel_dvad_prt_cnt AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        ar_trx_key,
        ship_to_customer_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_gl_rev_channel_dvad_prt_cnt') }}
),

source_wi_gl_rev_channel_measure AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        rtm_type,
        ar_trx_key
    FROM {{ source('raw', 'wi_gl_rev_channel_measure') }}
),

source_wi_gl_rev_channel_dvad_partner AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk_as_is,
        deals_ppsk,
        ar_trx_key,
        ship_to_customer_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_gl_rev_channel_dvad_partner') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk_as_is,
        deals_ppsk,
        ar_trx_key,
        ship_to_customer_key,
        bill_to_customer_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM source_wi_gl_rev_channel_dvad_partner
)

SELECT * FROM final