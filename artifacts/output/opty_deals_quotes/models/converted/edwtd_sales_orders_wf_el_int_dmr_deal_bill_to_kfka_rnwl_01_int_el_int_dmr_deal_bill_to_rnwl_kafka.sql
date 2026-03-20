{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_dmr_deal_bill_to_kfka_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_DMR_DEAL_BILL_TO_KFKA_RNWL',
        'target_table': 'EL_INT_DMR_DEAL_BILL_TO_RNWL_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.904681+00:00'
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
    FROM source_el_int_dmr_deal_bill_to_rnwl_kafka
)

SELECT * FROM final