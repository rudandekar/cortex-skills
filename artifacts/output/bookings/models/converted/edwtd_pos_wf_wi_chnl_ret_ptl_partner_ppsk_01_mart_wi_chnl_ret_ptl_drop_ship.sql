{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_chnl_ret_ptl_partner_ppsk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_CHNL_RET_PTL_PARTNER_PPSK',
        'target_table': 'WI_CHNL_RET_PTL_DROP_SHIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.357560+00:00'
    }
) }}

WITH 

source_wi_chnl_ret_ptl_drop_ship AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        end_cust_country,
        drop_ship_party_site_key
    FROM {{ source('raw', 'wi_chnl_ret_ptl_drop_ship') }}
),

source_wi_chnl_ret_ptl_part_country AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        end_cust_country
    FROM {{ source('raw', 'wi_chnl_ret_ptl_part_country') }}
),

source_n_channel_partner_site AS (
    SELECT
        partner_site_party_key,
        bk_party_id_int,
        partner_country_party_key,
        dd_grndparnt_partner_party_key,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'n_channel_partner_site') }}
),

final AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_party_key,
        end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        end_cust_country,
        drop_ship_party_site_key
    FROM source_n_channel_partner_site
)

SELECT * FROM final