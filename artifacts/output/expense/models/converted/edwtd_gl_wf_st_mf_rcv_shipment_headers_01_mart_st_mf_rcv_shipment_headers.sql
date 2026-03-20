{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_rcv_shipment_headers', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_RCV_SHIPMENT_HEADERS',
        'target_table': 'ST_MF_RCV_SHIPMENT_HEADERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.155782+00:00'
    }
) }}

WITH 

source_ff_mf_rcv_shipment_headers AS (
    SELECT
        batch_id,
        attribute1,
        attribute2,
        attribute15,
        bill_of_lading,
        comments,
        created_by,
        creation_date,
        employee_id,
        expected_receipt_date,
        freight_carrier_code,
        last_update_date,
        last_update_login,
        last_updated_by,
        num_of_containers,
        organization_id,
        packing_slip,
        receipt_num,
        receipt_source_code,
        ship_to_location_id,
        ship_to_org_id,
        shipment_header_id,
        shipment_num,
        shipped_date,
        vendor_id,
        vendor_site_id,
        waybill_airbill_num,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_rcv_shipment_headers') }}
),

final AS (
    SELECT
        batch_id,
        attribute1,
        attribute2,
        attribute15,
        bill_of_lading,
        comments,
        created_by,
        creation_date,
        employee_id,
        expected_receipt_date,
        freight_carrier_code,
        last_update_date,
        last_update_login,
        last_updated_by,
        num_of_containers,
        organization_id,
        packing_slip,
        receipt_num,
        receipt_source_code,
        ship_to_location_id,
        ship_to_org_id,
        shipment_header_id,
        shipment_num,
        shipped_date,
        vendor_id,
        vendor_site_id,
        waybill_airbill_num,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM source_ff_mf_rcv_shipment_headers
)

SELECT * FROM final