{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_appl_ar_trx_line_assgn_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WK_APPL_AR_TRX_LINE_ASSGN_OOD',
        'target_table': 'EX_APPL_AR_TRX_LINE_ASSGN_OOD_FUSN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.754692+00:00'
    }
) }}

WITH 

source_st_ood_fusn_ra_cust_trx_lines AS (
    SELECT
        accounting_rule_duration,
        accounting_rule_id,
        attribute15,
        attribute3,
        attribute4,
        customer_trx_id,
        customer_trx_line_id,
        description,
        extended_amount,
        global_attribute7,
        global_order_number,
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute14,
        interface_line_attribute15,
        interface_line_attribute2,
        global_line_id,
        interface_line_attribute8,
        interface_line_context,
        global_product_id,
        line_number,
        line_type,
        link_to_cust_trx_line_id,
        org_id,
        previous_customer_trx_line_id,
        quantity_credited,
        quantity_invoiced,
        reason_code,
        rule_start_date,
        tax_rate,
        unit_standard_price,
        unit_selling_price,
        uom_code,
        attribute1,
        attribute2,
        interface_line_attribute6,
        inventory_item_id,
        creation_date,
        last_update_date,
        rule_end_date,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_fusn_ra_cust_trx_lines') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_sm_ar_trx_line AS (
    SELECT
        ar_trx_line_key,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_ar_trx_line') }}
),

source_sm_sales_order_line AS (
    SELECT
        sales_order_line_key,
        ss_code,
        sk_so_line_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line') }}
),

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
        accounting_rule_duration,
        accounting_rule_id,
        attribute15,
        attribute3,
        attribute4,
        customer_trx_id,
        customer_trx_line_id,
        description,
        extended_amount,
        global_attribute7,
        global_order_number,
        interface_line_attribute11,
        interface_line_attribute12,
        interface_line_attribute14,
        interface_line_attribute15,
        interface_line_attribute2,
        global_line_id,
        interface_line_attribute8,
        interface_line_context,
        global_product_id,
        line_number,
        line_type,
        link_to_cust_trx_line_id,
        org_id,
        previous_customer_trx_line_id,
        quantity_credited,
        quantity_invoiced,
        reason_code,
        rule_start_date,
        tax_rate,
        unit_standard_price,
        unit_selling_price,
        uom_code,
        attribute1,
        attribute2,
        interface_line_attribute6,
        inventory_item_id,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_ood_ra_cust_trx_lines
)

SELECT * FROM final