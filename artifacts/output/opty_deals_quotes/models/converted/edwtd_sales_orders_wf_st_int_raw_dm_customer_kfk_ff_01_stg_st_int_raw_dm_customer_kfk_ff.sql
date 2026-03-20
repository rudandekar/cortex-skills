{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_dm_customer_kfk_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_DM_CUSTOMER_KFK_FF',
        'target_table': 'ST_INT_RAW_DM_CUSTOMER_KFK_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.912095+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_dm_customer_kafka AS (
    SELECT
        parent_id,
        object_id,
        cr_id,
        account_name,
        addr,
        addr_line_2,
        addr_line_3,
        city,
        province,
        zipcode,
        country,
        county,
        end_customer_type,
        siebel_company_id,
        structured_account,
        account_type,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_dm_customer_kafka') }}
),

final AS (
    SELECT
        parent_id,
        object_id,
        cr_id,
        account_name,
        addr,
        addr_line_2,
        addr_line_3,
        city,
        province,
        zipcode,
        country,
        county,
        end_customer_type,
        siebel_company_id,
        structured_account,
        account_type,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        provinceorstate
    FROM source_ff_st_int_raw_dm_customer_kafka
)

SELECT * FROM final