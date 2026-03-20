{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_matrix_dvad', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MATRIX_DVAD',
        'target_table': 'WI_SALES_CHANNEL_MATRIX_DVAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.456291+00:00'
    }
) }}

WITH 

source_wi_sales_chnl_mtx_ptrns_dvad AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        sales_order_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_ptrns_dvad') }}
),

source_wi_sls_chnl_mtx_drop_ship_dvad AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        deals_party_site_key,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_drop_ship_dvad') }}
),

source_wi_sales_chnl_mtx_prtnr_dvad AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_prtnr_dvad') }}
),

source_wi_sales_chnl_mtx_country_dvad AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        dv_end_cust_country_cd,
        deals_cntry_cd,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_country_dvad') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        dv_drct_val_added_dsti_ord_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_wi_sales_chnl_mtx_country_dvad
)

SELECT * FROM final