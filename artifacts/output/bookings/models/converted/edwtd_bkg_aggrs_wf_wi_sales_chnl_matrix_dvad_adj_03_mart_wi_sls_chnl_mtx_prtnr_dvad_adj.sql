{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_chnl_matrix_dvad_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHNL_MATRIX_DVAD_ADJ',
        'target_table': 'WI_SLS_CHNL_MTX_PRTNR_DVAD_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.934988+00:00'
    }
) }}

WITH 

source_wi_sales_chnl_matrix_dvad_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        dv_drct_val_added_dsti_ord_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_matrix_dvad_adj') }}
),

source_wi_sls_chnl_mtx_prtnr_dvad_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_prtnr_dvad_adj') }}
),

source_wi_sls_chnl_mtx_cntry_dvad_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        end_cust_country_cd,
        deals_cntry_cd,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_cntry_dvad_adj') }}
),

source_wi_sls_chnl_mtx_dship_dvad_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        end_cust_country_cd,
        deals_cntry_cd,
        deals_party_site_key,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_dship_dvad_adj') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        end_cust_party_key,
        deals_ppsk,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_wi_sls_chnl_mtx_dship_dvad_adj
)

SELECT * FROM final