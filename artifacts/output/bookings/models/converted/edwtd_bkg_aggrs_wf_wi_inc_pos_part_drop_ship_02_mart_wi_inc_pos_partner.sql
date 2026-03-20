{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_inc_pos_part_drop_ship', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_INC_POS_PART_DROP_SHIP',
        'target_table': 'WI_INC_POS_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.108788+00:00'
    }
) }}

WITH 

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

source_wi_inc_pos_partner AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        dv_bk_end_user_site_use_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key
    FROM {{ source('raw', 'wi_inc_pos_partner') }}
),

source_wi_inc_pos_patterns AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        dv_bk_end_user_site_use_key,
        sales_territory_key
    FROM {{ source('raw', 'wi_inc_pos_patterns') }}
),

source_wi_inc_pos_partner_country AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        dv_bk_end_user_site_use_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        dv_end_cust_country
    FROM {{ source('raw', 'wi_inc_pos_partner_country') }}
),

source_n_wips_customer_site_tv AS (
    SELECT
        wips_site_use_key,
        start_tv_date,
        end_tv_date,
        bk_site_use_id_int,
        match_score_code,
        match_scoring_criteria_code,
        wips_customer_site_use_type,
        customer_party_key,
        wips_address_id_int,
        wips_customer_id_int,
        ep_cr_party_id_int,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'n_wips_customer_site_tv') }}
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
        edw_update_datetime
    FROM {{ source('raw', 'n_postal_address_locator') }}
),

final AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        dv_bk_end_user_site_use_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        bk_wips_originator_id_int,
        bk_ship_to_wips_site_use_key
    FROM source_n_postal_address_locator
)

SELECT * FROM final