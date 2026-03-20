{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_recurring_offer_bookings', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_RECURRING_OFFER_BOOKINGS',
        'target_table': 'MT_RECURRING_OFFER_BOOKINGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.292856+00:00'
    }
) }}

WITH 

source_wi_recurring_offer_bookings AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        enterprise_sku_prdt_key,
        dv_ro_product_key,
        bk_offer_type_name,
        cscc_flg,
        ela_flg,
        xcat_flg,
        recurring_offer_flg,
        dv_ru_software_usage_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        corporate_recurring_bkgs_flg,
        dv_deal_id,
        dv_source_order_num_int
    FROM {{ source('raw', 'wi_recurring_offer_bookings') }}
),

final AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        enterprise_sku_prdt_key,
        dv_ro_product_key,
        bk_offer_type_name,
        cscc_flg,
        ela_flg,
        xcat_flg,
        recurring_offer_flg,
        dv_ru_software_usage_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        corporate_recurring_bkgs_flg,
        dv_deal_id,
        dv_source_order_num_int
    FROM source_wi_recurring_offer_bookings
)

SELECT * FROM final