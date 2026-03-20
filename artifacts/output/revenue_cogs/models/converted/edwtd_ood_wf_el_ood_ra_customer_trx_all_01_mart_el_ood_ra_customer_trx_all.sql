{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_ra_customer_trx_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_RA_CUSTOMER_TRX_ALL',
        'target_table': 'EL_OOD_RA_CUSTOMER_TRX_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.758266+00:00'
    }
) }}

WITH 

source_st_ood_ra_customer_trx_all AS (
    SELECT
        org_id,
        customer_trx_id,
        trx_number,
        cust_trx_type_id,
        trx_date,
        batch_source_id,
        reason_code,
        sold_to_customer_number,
        bill_to_customer_number,
        ship_to_customer_number,
        previous_customer_trx_id,
        invoice_currency_code,
        attribute10,
        request_id,
        complete_flag,
        invoicing_rule_id,
        interface_header_attribute1,
        interface_header_context,
        creation_date,
        interface_header_attribute10,
        interface_header_attribute11,
        interface_header_attribute9,
        term_id,
        purchase_order,
        last_update_date,
        billto_location_number,
        shipto_location_number,
        create_datetime,
        action_code,
        exchange_rate,
        conversion_date,
        deal_id
    FROM {{ source('raw', 'st_ood_ra_customer_trx_all') }}
),

final AS (
    SELECT
        org_id,
        customer_trx_id,
        trx_number,
        cust_trx_type_id,
        trx_date,
        batch_source_id,
        reason_code,
        sold_to_customer_number,
        bill_to_customer_number,
        ship_to_customer_number,
        previous_customer_trx_id,
        invoice_currency_code,
        attribute10,
        request_id,
        complete_flag,
        invoicing_rule_id,
        interface_header_attribute1,
        interface_header_context,
        creation_date,
        interface_header_attribute10,
        interface_header_attribute11,
        interface_header_attribute9,
        term_id,
        purchase_order,
        last_update_date,
        billto_location_number,
        shipto_location_number,
        edw_update_dtm,
        exchange_rate,
        conversion_date,
        identifier
    FROM source_st_ood_ra_customer_trx_all
)

SELECT * FROM final