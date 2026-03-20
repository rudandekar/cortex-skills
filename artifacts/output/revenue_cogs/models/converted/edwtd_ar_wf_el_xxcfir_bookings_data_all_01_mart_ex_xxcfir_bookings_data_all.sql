{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_bookings_data_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_BOOKINGS_DATA_ALL',
        'target_table': 'EX_XXCFIR_BOOKINGS_DATA_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.863781+00:00'
    }
) }}

WITH 

source_ex_xxcfir_bookings_data_all AS (
    SELECT
        unique_event_id,
        transaction_source,
        subscriber1,
        event_name,
        order_header_id,
        order_line_id,
        parent_order_line_id,
        inventory_item_id,
        ordered_item,
        accounting_rule,
        accounting_rule_duration,
        override_flag,
        offer_code,
        subscription_code,
        subscription_ref_id,
        charge_type,
        subscription_start_date,
        previous_initial_term,
        previous_renewal_term,
        trx_id,
        attribution_id,
        attribution_code,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        exception_type
    FROM {{ source('raw', 'ex_xxcfir_bookings_data_all') }}
),

source_el_xxcfir_bookings_data_all AS (
    SELECT
        transaction_seq_id,
        trx_src_cd,
        sales_order_key,
        sales_order_line_key,
        top_sku_sales_order_line_key,
        sk_inventory_item_id,
        bk_product_id,
        accounting_rule,
        accounting_rule_duration,
        override_flag,
        offer_code,
        subscription_code,
        subscription_ref_id,
        charge_type,
        subscription_start_date,
        previos_initial_term,
        previous_renewal_term,
        trx_id,
        attribution_id,
        attribution_code,
        item_key,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user
    FROM {{ source('raw', 'el_xxcfir_bookings_data_all') }}
),

final AS (
    SELECT
        unique_event_id,
        transaction_source,
        subscriber1,
        event_name,
        order_header_id,
        order_line_id,
        parent_order_line_id,
        inventory_item_id,
        ordered_item,
        accounting_rule,
        accounting_rule_duration,
        override_flag,
        offer_code,
        subscription_code,
        subscription_ref_id,
        charge_type,
        subscription_start_date,
        previous_initial_term,
        previous_renewal_term,
        trx_id,
        attribution_id,
        attribution_code,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        exception_type
    FROM source_el_xxcfir_bookings_data_all
)

SELECT * FROM final