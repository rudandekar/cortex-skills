{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dmr_deal_bill_to_kafka_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DMR_DEAL_BILL_TO_KAFKA_RNWL',
        'target_table': 'ST_INT_DMR_DEAL_BILL_TO_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.942730+00:00'
    }
) }}

WITH 

source_el_int_dmr_deal_bill_to_rnwl_kafka AS (
    SELECT
        object_id,
        quote_object_id,
        billto_location,
        customer_name,
        cust_account_id,
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
        edw_update_user,
        edw_update_dtm,
        quote_status_id,
        ref_entity_number
    FROM {{ source('raw', 'el_int_dmr_deal_bill_to_rnwl_kafka') }}
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
        edw_updated_date
    FROM source_el_int_dmr_deal_bill_to_rnwl_kafka
)

SELECT * FROM final