{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_matrix_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MATRIX_ADJ',
        'target_table': 'WI_SALES_CHANNEL_MATRIX_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.181751+00:00'
    }
) }}

WITH 

source_wi_sales_channel_matrix_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_channel_matrix_adj') }}
),

source_wi_sales_chnl_matrix_int_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key,
        level_1_sls_hierarchy,
        partner_type_code,
        rule_cd,
        sold_to_guk,
        end_cust_guk,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_matrix_int_adj') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_wi_sales_chnl_matrix_int_adj
)

SELECT * FROM final