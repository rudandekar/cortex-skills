{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_so_nrt_retro', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_SO_NRT_RETRO',
        'target_table': 'WI_NSON_NRT_RETRO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.621848+00:00'
    }
) }}

WITH 

source_wi_nson_nrt_retro AS (
    SELECT
        sales_order_key,
        default_ship_to_customer_key,
        sales_order_type_name,
        sales_order_source_type_cd,
        so_operating_unit_name_cd,
        ep_stc_ship_to_org_id_int,
        order_source_id,
        order_type_id,
        ep_so_oprtng_unit_org_id_int,
        global_name
    FROM {{ source('raw', 'wi_nson_nrt_retro') }}
),

source_wi_so_nrt_retro_1 AS (
    SELECT
        sales_order_key,
        so_source_update_dtm,
        bill_to_customer_key,
        ep_btc_inv_to_org_id_int,
        sold_to_customer_account_key,
        ep_stc_sold_to_org_id_int,
        source_commit_dtm
    FROM {{ source('raw', 'wi_so_nrt_retro_1') }}
),

source_wi_so_nrt_retro_2 AS (
    SELECT
        sales_order_key,
        so_source_update_dtm,
        bill_to_customer_key,
        ep_btc_inv_to_org_id_int,
        sold_to_customer_account_key,
        ep_stc_sold_to_org_id_int,
        source_commit_dtm,
        source_commit_date,
        new_bill_to_customer_key,
        new_sold_to_customer_acct_key
    FROM {{ source('raw', 'wi_so_nrt_retro_2') }}
),

source_n_sales_order_nrt_hist_tv AS (
    SELECT
        sales_order_key,
        sales_order_category_type,
        sk_sales_order_header_id_int,
        ss_cd,
        cisco_booked_result_cd,
        so_source_update_dtm,
        dv_so_source_update_dt,
        sales_channel_source_type,
        sales_channel_cd,
        bill_to_customer_key,
        ep_btc_inv_to_org_id_int,
        sold_to_customer_account_key,
        ep_stc_sold_to_org_id_int,
        transactional_currency_cd,
        source_commit_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_sales_order_nrt_hist_tv') }}
),

final AS (
    SELECT
        sales_order_key,
        default_ship_to_customer_key,
        sales_order_type_name,
        sales_order_source_type_cd,
        so_operating_unit_name_cd,
        ep_stc_ship_to_org_id_int,
        order_source_id,
        order_type_id,
        ep_so_oprtng_unit_org_id_int,
        global_name
    FROM source_n_sales_order_nrt_hist_tv
)

SELECT * FROM final