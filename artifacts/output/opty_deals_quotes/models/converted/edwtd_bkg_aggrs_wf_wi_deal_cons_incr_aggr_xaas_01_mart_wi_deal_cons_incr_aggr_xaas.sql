{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_deal_cons_incr_aggr_xaas', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_DEAL_CONS_INCR_AGGR_XAAS',
        'target_table': 'WI_DEAL_CONS_INCR_AGGR_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.874442+00:00'
    }
) }}

WITH 

source_wi_deal_cons_incr_aggr_xaas AS (
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
        web_order_id
    FROM {{ source('raw', 'wi_deal_cons_incr_aggr_xaas') }}
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
        web_order_id
    FROM source_wi_deal_cons_incr_aggr_xaas
)

SELECT * FROM final