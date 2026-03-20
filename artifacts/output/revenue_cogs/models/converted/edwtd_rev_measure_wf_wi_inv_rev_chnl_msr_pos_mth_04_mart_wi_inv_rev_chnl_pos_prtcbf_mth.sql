{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_inv_rev_chnl_msr_pos_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_INV_REV_CHNL_MSR_POS_MTH',
        'target_table': 'WI_INV_REV_CHNL_POS_PRTCBF_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.700991+00:00'
    }
) }}

WITH 

source_wi_inv_rev_chnl_pos_prtcbf_mth AS (
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
        ar_trx_key
    FROM {{ source('raw', 'wi_inv_rev_chnl_pos_prtcbf_mth') }}
),

source_wi_inv_rev_chnl_pos_ptnr_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        ar_trx_key
    FROM {{ source('raw', 'wi_inv_rev_chnl_pos_ptnr_mth') }}
),

source_wi_inv_rev_chnl_pos_ptns_mth AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        ar_trx_key
    FROM {{ source('raw', 'wi_inv_rev_chnl_pos_ptns_mth') }}
),

source_wi_inv_rev_chnl_pos_pat_ct_mth AS (
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
        ar_trx_key
    FROM {{ source('raw', 'wi_inv_rev_chnl_pos_pat_ct_mth') }}
),

source_wi_inv_rev_chnl_pos_pspk_mth AS (
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
        ar_trx_key
    FROM {{ source('raw', 'wi_inv_rev_chnl_pos_pspk_mth') }}
),

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
        level_1_sls_hierarchy,
        channel_flg,
        ar_trx_key,
        ship_to_customer_key,
        bk_wips_originator_id_int
    FROM source_wi_inv_rev_channel_measure_mth
)

SELECT * FROM final