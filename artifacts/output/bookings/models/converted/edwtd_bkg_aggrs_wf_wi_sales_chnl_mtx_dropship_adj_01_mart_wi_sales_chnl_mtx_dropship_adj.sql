{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_chnl_mtx_dropship_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHNL_MTX_DROPSHIP_ADJ',
        'target_table': 'WI_SALES_CHNL_MTX_DROPSHIP_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.739586+00:00'
    }
) }}

WITH 

source_wi_sales_chnl_mtx_dropship_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        end_cust_party_key,
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        end_cust_cntry_cd,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_dropship_adj') }}
),

source_wi_chnl_mtx_drop_ship_int_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        end_cust_party_key,
        partner_site_party_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        end_cust_cntry_cd,
        drop_ship_party_site_key,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_chnl_mtx_drop_ship_int_adj') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        end_cust_party_key,
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        end_cust_cntry_cd,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        ship_to_customer_key
    FROM source_wi_chnl_mtx_drop_ship_int_adj
)

SELECT * FROM final