{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_mtx_drop_ship', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MTX_DROP_SHIP',
        'target_table': 'WI_SALES_CHANNEL_MTX_DROP_SHIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.749212+00:00'
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

source_wi_sales_channel_mtx_country AS (
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
        dv_end_cust_cntry_cd
    FROM {{ source('raw', 'wi_sales_channel_mtx_country') }}
),

source_wi_channel_mtx_drop_ship_int AS (
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
        partner_site_party_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        drop_ship_party_site_key
    FROM {{ source('raw', 'wi_channel_mtx_drop_ship_int') }}
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
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key,
        ship_to_customer_key,
        partner_bill_to_cust_party_key
    FROM source_wi_channel_mtx_drop_ship_int
)

SELECT * FROM final