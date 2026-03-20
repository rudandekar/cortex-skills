{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_dm_custmer_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_DM_CUSTMER_KAFKA',
        'target_table': 'EL_INT_RAW_DM_CUSTOMER_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.944915+00:00'
    }
) }}

WITH 

source_el_int_raw_dm_customer_kafka1 AS (
    SELECT
        object_id,
        deal_object_id,
        siebel_company_id,
        csc_comp_target_id,
        csc_site_id,
        csc_global_ultimate_id,
        account_type,
        structured_account,
        account_name,
        addr,
        addr_line_2,
        addr_line_3,
        city,
        province,
        zipcode,
        country,
        end_customer_type,
        created_by,
        created_on,
        updated_by,
        updated_on,
        cr_id,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        county,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_raw_dm_customer_kafka1') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        siebel_company_id,
        csc_comp_target_id,
        csc_site_id,
        csc_global_ultimate_id,
        account_type,
        structured_account,
        account_name,
        addr,
        addr_line_2,
        addr_line_3,
        city,
        province,
        zipcode,
        country,
        end_customer_type,
        created_by,
        created_on,
        updated_by,
        updated_on,
        cr_id,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        county,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_raw_dm_customer_kafka1
)

SELECT * FROM final