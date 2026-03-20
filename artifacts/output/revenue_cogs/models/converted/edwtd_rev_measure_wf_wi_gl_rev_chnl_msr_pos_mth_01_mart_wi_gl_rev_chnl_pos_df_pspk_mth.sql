{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_chnl_msr_pos_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_CHNL_MSR_POS_MTH',
        'target_table': 'WI_GL_REV_CHNL_POS_DF_PSPK_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.312583+00:00'
    }
) }}

WITH 

source_wi_gl_rev_channel_pos_part_cbf AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        level_1_sls_hierarchy,
        channel_flg,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM {{ source('raw', 'wi_gl_rev_channel_pos_part_cbf') }}
),

source_wi_gl_rev_channel_pos_patterns AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM {{ source('raw', 'wi_gl_rev_channel_pos_patterns') }}
),

source_wi_gl_rev_channel_pos_dsf_pspk AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM {{ source('raw', 'wi_gl_rev_channel_pos_dsf_pspk') }}
),

source_wi_gl_rev_channel_pos_partners AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM {{ source('raw', 'wi_gl_rev_channel_pos_partners') }}
),

source_wi_gl_rev_channel_pos_pt_conty AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM {{ source('raw', 'wi_gl_rev_channel_pos_pt_conty') }}
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

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM source_wi_gl_rev_channel_measure
)

SELECT * FROM final