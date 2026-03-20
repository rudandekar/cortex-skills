{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_dmr_deal_bill_to_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_DMR_DEAL_BILL_TO_KFKA',
        'target_table': 'el_int_dmr_deal_bill_to_kafka',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.968100+00:00'
    }
) }}

WITH 

source_el_int_dmr_deal_bill_to_kafka AS (
    SELECT
        object_id,
        quote_object_id,
        billto_location,
        party_id,
        customer_name,
        cust_acct_site_id,
        cust_account_id,
        account_number,
        address1,
        address2,
        address3,
        address4,
        country,
        city,
        postal_code,
        state,
        province,
        county,
        country_desc,
        created_on,
        created_by,
        updated_on,
        updated_by,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_dmr_deal_bill_to_kafka') }}
),

final AS (
    SELECT
        object_id,
        quote_object_id,
        billto_location,
        party_id,
        customer_name,
        cust_acct_site_id,
        cust_account_id,
        account_number,
        address1,
        address2,
        address3,
        address4,
        country,
        city,
        postal_code,
        state,
        province,
        county,
        country_desc,
        created_on,
        created_by,
        updated_on,
        updated_by,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_dmr_deal_bill_to_kafka
)

SELECT * FROM final