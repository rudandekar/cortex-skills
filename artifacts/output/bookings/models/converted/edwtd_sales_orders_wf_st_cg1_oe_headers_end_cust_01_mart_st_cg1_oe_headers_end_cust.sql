{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_headers_end_cust', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_HEADERS_END_CUST',
        'target_table': 'ST_CG1_OE_HEADERS_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.806517+00:00'
    }
) }}

WITH 

source_cg1_oe_order_headers_all AS (
    SELECT
        batch_id,
        order_number,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute9,
        attribute10,
        attribute12,
        attribute18,
        attribute19,
        attribute20,
        booked_date,
        cancelled_flag,
        conversion_type_code,
        created_by,
        creation_date,
        cust_po_number,
        demand_class_code,
        end_customer_site_use_id,
        end_customer_id,
        flow_status_code,
        fob_point_code,
        freight_carrier_code,
        header_id,
        invoice_to_contact_id,
        invoice_to_org_id,
        last_update_date,
        last_updated_by,
        open_flag,
        order_category_code,
        order_source_id,
        order_type_id,
        ordered_date,
        org_id,
        orig_sys_document_ref,
        packing_instructions,
        partial_shipments_allowed,
        payment_term_id,
        price_list_id,
        pricing_date,
        sales_channel_code,
        ship_from_org_id,
        ship_to_contact_id,
        ship_to_org_id,
        shipment_priority_code,
        shipping_instructions,
        sold_to_contact_id,
        sold_to_org_id,
        source_document_id,
        tax_exempt_flag,
        transactional_curr_code,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        old_invoice_to_org_id,
        old_sales_channel_code,
        old_sold_to_org_id,
        old_transactional_curr_code,
        attribute11,
        attribute17,
        attribute13,
        return_reason_code,
        order_date_type_code,
        attribute3,
        end_customer_contact_id
    FROM {{ source('raw', 'cg1_oe_order_headers_all') }}
),

final AS (
    SELECT
        header_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        global_name
    FROM source_cg1_oe_order_headers_all
)

SELECT * FROM final