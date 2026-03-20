{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_deal_consumption_aggr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_DEAL_CONSUMPTION_AGGR',
        'target_table': 'WI_DEAL_CONSUMPTION_AGGR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.874075+00:00'
    }
) }}

WITH 

source_wi_deal_consumption_aggr AS (
    SELECT
        deal_id,
        cisco_booked_date,
        sales_order_number,
        transactional_currency_cd,
        bkgs_measure_trans_type_code,
        operation,
        sales_channel_code,
        order_creation_date,
        order_usd_list_price_amount,
        order_local_list_price_amount,
        order_usd_net_price_amount,
        order_local_net_price_amount,
        product_usd_net_amount,
        product_local_net_amount,
        product_usd_list_amount,
        product_local_list_amount,
        service_usd_net_amount,
        service_local_net_amount,
        service_usd_list_amount,
        service_local_list_amount,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        web_order_id,
        order_usd_standard_list_price,
        order_local_standard_list_price
    FROM {{ source('raw', 'wi_deal_consumption_aggr') }}
),

final AS (
    SELECT
        deal_id,
        cisco_booked_date,
        sales_order_number,
        transactional_currency_cd,
        bkgs_measure_trans_type_code,
        operation,
        sales_channel_code,
        order_creation_date,
        order_usd_list_price_amount,
        order_local_list_price_amount,
        order_usd_net_price_amount,
        order_local_net_price_amount,
        product_usd_net_amount,
        product_local_net_amount,
        product_usd_list_amount,
        product_local_list_amount,
        service_usd_net_amount,
        service_local_net_amount,
        service_usd_list_amount,
        service_local_list_amount,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        web_order_id,
        order_usd_standard_list_price,
        order_local_standard_list_price
    FROM source_wi_deal_consumption_aggr
)

SELECT * FROM final