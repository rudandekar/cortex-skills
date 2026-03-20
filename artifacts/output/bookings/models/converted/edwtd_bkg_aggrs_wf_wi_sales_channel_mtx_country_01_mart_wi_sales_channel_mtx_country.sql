{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_mtx_country', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MTX_COUNTRY',
        'target_table': 'WI_SALES_CHANNEL_MTX_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.623684+00:00'
    }
) }}

WITH 

source_wi_sales_channel_mtx_partner AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_party_key,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'wi_sales_channel_mtx_partner') }}
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
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        ship_to_customer_key,
        partner_bill_to_cust_party_key
    FROM source_n_postal_address_locator
)

SELECT * FROM final