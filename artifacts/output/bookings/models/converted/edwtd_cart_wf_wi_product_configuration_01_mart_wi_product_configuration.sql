{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_product_configuration', 'batch', 'edwtd_cart'],
    meta={
        'source_workflow': 'wf_m_WI_PRODUCT_CONFIGURATION',
        'target_table': 'WI_PRODUCT_CONFIGURATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.000237+00:00'
    }
) }}

WITH 

source_wi_bkgs_prod_config_fmth AS (
    SELECT
        major_product_key,
        major_product_id,
        major_sales_order_line_key,
        minor_product_key,
        minor_product_id,
        dd_extended_quantity,
        dd_comp_us_net_price_amount,
        bk_so_number_int,
        sold_to_customer_key,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        major_minor_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_corporate_booking_flg
    FROM {{ source('raw', 'wi_bkgs_prod_config_fmth') }}
),

final AS (
    SELECT
        product_configuration_desc,
        major_product_key,
        major_sales_order_line_key,
        major_product_id,
        minor_product_key,
        rank_number
    FROM source_wi_bkgs_prod_config_fmth
)

SELECT * FROM final