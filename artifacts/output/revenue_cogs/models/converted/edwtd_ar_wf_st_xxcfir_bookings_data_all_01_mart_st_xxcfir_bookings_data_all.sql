{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfir_bookings_data_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFIR_BOOKINGS_DATA_ALL',
        'target_table': 'ST_XXCFIR_BOOKINGS_DATA_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.545994+00:00'
    }
) }}

WITH 

source_ff_xxcfir_bookings_data_all AS (
    SELECT
        unique_event_id,
        transaction_source,
        subscriber,
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
        last_updated_date
    FROM {{ source('raw', 'ff_xxcfir_bookings_data_all') }}
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
        last_updated_date
    FROM source_ff_xxcfir_bookings_data_all
)

SELECT * FROM final