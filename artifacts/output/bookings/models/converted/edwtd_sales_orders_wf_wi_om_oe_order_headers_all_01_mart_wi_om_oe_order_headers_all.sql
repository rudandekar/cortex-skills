{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_om_oe_order_headers_all', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OM_OE_ORDER_HEADERS_ALL',
        'target_table': 'WI_OM_OE_ORDER_HEADERS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.723049+00:00'
    }
) }}

WITH 

source_st_om_oe_order_headers_all AS (
    SELECT
        header_id,
        global_name,
        attribute6,
        packing_instructions,
        shipping_instructions,
        org_id,
        ship_to_contact_id,
        sold_to_contact_id,
        invoice_to_contact_id,
        order_number,
        ges_update_date,
        created_by,
        creation_date,
        demand_class_code,
        last_updated_by,
        last_update_date,
        attribute4,
        adl_attribute4,
        attribute5,
        attribute12,
        payment_term_id,
        cancelled_flag,
        cancel_order_date,
        batch_id,
        create_datetime,
        tax_exempt_flag,
        action_code,
        attribute7
    FROM {{ source('raw', 'st_om_oe_order_headers_all') }}
),

source_ex_om_oe_order_headers_all AS (
    SELECT
        header_id,
        global_name,
        attribute6,
        packing_instructions,
        shipping_instructions,
        org_id,
        ship_to_contact_id,
        sold_to_contact_id,
        invoice_to_contact_id,
        order_number,
        ges_update_date,
        created_by,
        creation_date,
        demand_class_code,
        last_updated_by,
        last_update_date,
        attribute4,
        adl_attribute4,
        attribute5,
        attribute12,
        payment_term_id,
        cancelled_flag,
        cancel_order_date,
        batch_id,
        create_datetime,
        action_code,
        tax_exempt_flag,
        exception_type,
        attribute7
    FROM {{ source('raw', 'ex_om_oe_order_headers_all') }}
),

final AS (
    SELECT
        header_id,
        global_name,
        attribute6,
        packing_instructions,
        shipping_instructions,
        org_id,
        ship_to_contact_id,
        sold_to_contact_id,
        invoice_to_contact_id,
        order_number,
        ges_update_date,
        created_by,
        creation_date,
        demand_class_code,
        last_updated_by,
        last_update_date,
        attribute4,
        adl_attribute4,
        attribute5,
        attribute12,
        payment_term_id,
        ru_so_cancelled_dtm,
        batch_id,
        create_datetime,
        tax_exempt_flag,
        action_code,
        attribute7,
        es_ordered_by_contact_id_int,
        es_bill_to_contact_id_int,
        es_ship_to_contact_id_int,
        ss_code,
        replacement_reason_code,
        return_reason_code
    FROM source_ex_om_oe_order_headers_all
)

SELECT * FROM final