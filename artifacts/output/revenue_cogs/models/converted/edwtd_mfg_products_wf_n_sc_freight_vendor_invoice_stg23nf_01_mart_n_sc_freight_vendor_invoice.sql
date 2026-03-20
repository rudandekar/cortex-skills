{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sc_freight_vendor_invoice__stg23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SC_FREIGHT_VENDOR_INVOICE__STG23NF',
        'target_table': 'N_SC_FREIGHT_VENDOR_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.206656+00:00'
    }
) }}

WITH 

source_st_weekly_run AS (
    SELECT
        run_number,
        carrier_name,
        d2l_service_type_description,
        mode_description,
        shipper_company,
        shipper_city,
        shipper_postal_code,
        shipper_state_province,
        shipper_country_code,
        shipper_port_code,
        consignee_company,
        consignee_city,
        consignee_postal_code,
        consignee_state_province,
        consignee_country_code,
        consignee_port_code,
        actual_weight,
        billed_weight,
        fuel_amount,
        all_other_accessorial,
        freight_amount,
        paid_amount,
        leg,
        notes,
        lane,
        bill_to_account_number,
        invoice_number,
        shipment_number,
        pieces,
        billed_amount,
        shipment_date,
        department,
        run_date,
        transaction_type,
        delivery_date,
        invoice_date,
        direction,
        bill_to_company,
        bill_to_name,
        bill_to_address_1,
        bill_to_address_2,
        bill_to_city,
        bill_to_state_province,
        bill_to_postal_code,
        bill_to_country_code,
        shipper_contact_name,
        shipper_address_1,
        shipper_address_2,
        shipper_account_number,
        shipper_reference_number,
        consignee_name,
        consignee_address_1,
        consignee_address_2,
        consignee_account_number,
        consignee_reference_number,
        actual_weight1,
        dimensional_weight,
        weight_qualifier,
        dimension_height,
        dimension_length,
        dimension_width,
        record_number
    FROM {{ source('raw', 'st_weekly_run') }}
),

final AS (
    SELECT
        bk_frght_carrier_name,
        bk_vendor_invoice_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_weekly_run
)

SELECT * FROM final