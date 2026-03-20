{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_customer_trx_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_CUSTOMER_TRX_ALL',
        'target_table': 'ST_OOD_FUSN_RA_CUSTOMER_TRX_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.962903+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_customer_trx_all AS (
    SELECT
        attribute10,
        batch_source_id,
        bill_to_customer_number,
        bill_to_location_number,
        creation_date,
        last_update_date,
        complete_flag,
        customer_trx_id,
        cust_trx_type_id,
        interface_header_attribute1,
        interface_header_attribute10,
        interface_header_attribute11,
        interface_header_attribute9,
        interface_header_context,
        invoice_currency_code,
        invoicing_rule_id,
        org_id,
        previous_customer_trx_id,
        purchase_order,
        reason_code,
        request_id,
        set_of_books_id,
        ship_to_customer_number,
        ship_to_location_number,
        sold_to_customer_number,
        term_id,
        trx_date,
        trx_number,
        interface_header_attribute6,
        bill_to_customer_id,
        ship_to_customer_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        exchange_rate,
        exchange_date,
        deal_id,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_customer_trx_all') }}
),

final AS (
    SELECT
        attribute10,
        batch_source_id,
        bill_to_customer_number,
        bill_to_location_number,
        creation_date,
        last_update_date,
        complete_flag,
        customer_trx_id,
        cust_trx_type_id,
        interface_header_attribute1,
        interface_header_attribute10,
        interface_header_attribute11,
        interface_header_attribute9,
        interface_header_context,
        invoice_currency_code,
        invoicing_rule_id,
        org_id,
        previous_customer_trx_id,
        purchase_order,
        reason_code,
        request_id,
        set_of_books_id,
        ship_to_customer_number,
        ship_to_location_number,
        sold_to_customer_number,
        term_id,
        trx_date,
        trx_number,
        interface_header_attribute6,
        bill_to_customer_id,
        ship_to_customer_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        exchange_rate,
        exchange_date,
        deal_id,
        split_key,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_customer_trx_all
)

SELECT * FROM final