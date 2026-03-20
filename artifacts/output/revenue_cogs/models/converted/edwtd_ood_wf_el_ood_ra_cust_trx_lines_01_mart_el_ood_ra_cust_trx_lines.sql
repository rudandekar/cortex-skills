{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_ra_cust_trx_lines', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_RA_CUST_TRX_LINES',
        'target_table': 'EL_OOD_RA_CUST_TRX_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.392634+00:00'
    }
) }}

WITH 

source_st_ood_ra_cust_trx_lines AS (
    SELECT
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute14,
        interface_line_attribute15,
        org_id,
        global_attribute7,
        customer_trx_line_id,
        customer_trx_id,
        reason_code,
        global_order_number,
        quantity_credited,
        quantity_invoiced,
        unit_standard_price,
        unit_selling_price,
        accounting_rule_id,
        line_type,
        rule_start_date,
        interface_line_context,
        global_product_id,
        interface_line_attribute2,
        global_line_id,
        extended_amount,
        link_to_cust_trx_line_id,
        attribute15,
        accounting_rule_duration,
        description,
        interface_line_attribute8,
        line_number,
        tax_rate,
        uom_code,
        creation_date,
        attribute3,
        attribute4,
        previous_customer_trx_line_id,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_ra_cust_trx_lines') }}
),

final AS (
    SELECT
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute14,
        interface_line_attribute15,
        org_id,
        global_attribute7,
        customer_trx_line_id,
        customer_trx_id,
        reason_code,
        global_order_number,
        quantity_credited,
        quantity_invoiced,
        unit_standard_price,
        unit_selling_price,
        accounting_rule_id,
        line_type,
        rule_start_date,
        interface_line_context,
        global_product_id,
        interface_line_attribute2,
        global_line_id,
        extended_amount,
        link_to_cust_trx_line_id,
        attribute15,
        accounting_rule_duration,
        description,
        interface_line_attribute8,
        line_number,
        tax_rate,
        uom_code,
        creation_date,
        attribute3,
        attribute4,
        previous_customer_trx_line_id,
        last_update_date,
        edw_update_dtm,
        identifier
    FROM source_st_ood_ra_cust_trx_lines
)

SELECT * FROM final