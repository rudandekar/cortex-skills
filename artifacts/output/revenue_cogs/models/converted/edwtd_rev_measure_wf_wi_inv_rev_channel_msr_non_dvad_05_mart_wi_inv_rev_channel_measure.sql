{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_inv_rev_channel_msr_non_dvad', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_INV_REV_CHANNEL_MSR_NON_DVAD',
        'target_table': 'WI_INV_REV_CHANNEL_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.409407+00:00'
    }
) }}

WITH 

source_wi_inv_rev_channel_patterns AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        ar_trx_key,
        partner_bill_to_cust_party_key,
        dv_ar_trx_line_key,
        trx_line_src_name
    FROM {{ source('raw', 'wi_inv_rev_channel_patterns') }}
),

source_wi_inv_rev_channel_measure AS (
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
    FROM {{ source('raw', 'wi_inv_rev_channel_measure') }}
),

source_wi_inv_rev_channel_prt_country AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        dv_end_customer_party_key,
        bill_to_party_key,
        sold_to_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        dv_end_cust_country,
        ar_trx_key,
        partner_bill_to_cust_party_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_inv_rev_channel_prt_country') }}
),

source_wi_inv_rev_channel_partner AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        dv_end_customer_party_key,
        bill_to_party_key,
        sold_to_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        ar_trx_key,
        partner_bill_to_cust_party_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_inv_rev_channel_partner') }}
),

source_wi_inv_rev_channel_part_pspk AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        dv_end_customer_party_key,
        bill_to_party_key,
        sold_to_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        ar_trx_key,
        partner_bill_to_cust_party_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_inv_rev_channel_part_pspk') }}
),

source_wi_inv_rev_channel_part_cbf AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        dv_end_customer_party_key,
        bill_to_party_key,
        sold_to_party_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key,
        level_1_sls_hierarchy,
        partner_type_code,
        rule_cd,
        sold_to_guk,
        end_cust_guk,
        channel_flg,
        edw_create_user,
        edw_create_datetime,
        ar_trx_key,
        partner_bill_to_cust_party_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'wi_inv_rev_channel_part_cbf') }}
),

final AS (
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
    FROM source_wi_inv_rev_channel_part_cbf
)

SELECT * FROM final