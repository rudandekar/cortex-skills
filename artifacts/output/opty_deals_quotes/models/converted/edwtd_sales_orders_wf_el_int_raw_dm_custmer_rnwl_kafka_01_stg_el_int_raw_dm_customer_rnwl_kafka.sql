{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_dm_custmer_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_DM_CUSTMER_RNWL_KAFKA',
        'target_table': 'EL_INT_RAW_DM_CUSTOMER_RNWL_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.889617+00:00'
    }
) }}

WITH 

source_el_int_raw_dm_customer_rnwl_kafka AS (
    SELECT
        object_id,
        deal_object_id,
        account_name,
        addr,
        addr_line_2,
        city,
        province,
        zipcode,
        country,
        county,
        country_code,
        cr_id,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_raw_dm_customer_rnwl_kafka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        account_name,
        addr,
        addr_line_2,
        city,
        province,
        zipcode,
        country,
        county,
        country_code,
        cr_id,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_raw_dm_customer_rnwl_kafka
)

SELECT * FROM final