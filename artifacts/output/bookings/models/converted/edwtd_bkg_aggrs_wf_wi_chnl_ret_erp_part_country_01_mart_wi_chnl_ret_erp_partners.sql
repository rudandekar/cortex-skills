{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_chnl_ret_erp_part_country', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_CHNL_RET_ERP_PART_COUNTRY',
        'target_table': 'WI_CHNL_RET_ERP_PARTNERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.520348+00:00'
    }
) }}

WITH 

source_wi_chnl_ret_erp_patterns AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key
    FROM {{ source('raw', 'wi_chnl_ret_erp_patterns') }}
),

source_wi_chnl_ret_erp_partners AS (
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
        dd_grndparnt_partner_party_key
    FROM {{ source('raw', 'wi_chnl_ret_erp_partners') }}
),

source_n_channel_partner_site AS (
    SELECT
        partner_site_party_key,
        bk_party_id_int,
        partner_country_party_key,
        dd_grndparnt_partner_party_key,
        edw_create_user,
        edw_create_datetime,
        source_deleted_flg
    FROM {{ source('raw', 'n_channel_partner_site') }}
),

source_n_erp_party AS (
    SELECT
        erp_party_number,
        cr_reported_party_id_int,
        erp_customer_role,
        erp_vendor_role,
        erp_party_name,
        erp_active_flag,
        sk_erp_customer_id,
        party_key,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        edw_observation_datetime
    FROM {{ source('raw', 'n_erp_party') }}
),

source_n_postal_address_locator AS (
    SELECT
        locator_key,
        line_1_address,
        line_2_address,
        line_3_address,
        line_4_address,
        city_name,
        state_or_province_name,
        county_name,
        postal_code,
        postal_plus4_code,
        iso_country_code,
        address_status_code,
        geo_validity_code,
        postal_restriction_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        postal_address_descr,
        postal_address_short_descr
    FROM {{ source('raw', 'n_postal_address_locator') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        bill_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        dv_end_customer_party_key,
        bill_to_party_key,
        sold_to_party_key,
        partner_site_party_key_int_as_is,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        ship_to_customer_key,
        partner_bill_to_cust_party_key
    FROM source_n_postal_address_locator
)

SELECT * FROM final