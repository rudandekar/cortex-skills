{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dmr_dl_bill_to_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DMR_DL_BILL_TO_KAFKA_FF',
        'target_table': 'ST_INT_DMR_DL_BILL_TO_KAFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.912739+00:00'
    }
) }}

WITH 

source_st_int_dmr_deal_bill_to_kafka_ff AS (
    SELECT
        parent_id,
        billto_location,
        customer_name,
        cust_acct_site_id,
        account_number,
        party_id,
        cust_account_id,
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
        object_id,
        created_on,
        created_by,
        updated_on,
        updated_by,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_int_dmr_deal_bill_to_kafka_ff') }}
),

final AS (
    SELECT
        parent_id,
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
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        object_id,
        created_on,
        created_by,
        updated_on,
        updated_by,
        edw_updated_date
    FROM source_st_int_dmr_deal_bill_to_kafka_ff
)

SELECT * FROM final