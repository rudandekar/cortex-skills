{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_transaction_line_pspk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_POS_TRANSACTION_LINE_PSPK',
        'target_table': 'WI_WIPS_POS_TRANS_PARTNERS_CTY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.469499+00:00'
    }
) }}

WITH 

source_wi_wips_pos_trans_partners AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key
    FROM {{ source('raw', 'wi_wips_pos_trans_partners') }}
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
        iso_country_code,
        postal_code,
        postal_plus4_code,
        address_status_code,
        geo_validity_code,
        postal_restriction_code,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'n_postal_address_locator') }}
),

source_wi_wips_pos_trans_partners_cty AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        end_cust_country
    FROM {{ source('raw', 'wi_wips_pos_trans_partners_cty') }}
),

final AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        end_cust_country
    FROM source_wi_wips_pos_trans_partners_cty
)

SELECT * FROM final