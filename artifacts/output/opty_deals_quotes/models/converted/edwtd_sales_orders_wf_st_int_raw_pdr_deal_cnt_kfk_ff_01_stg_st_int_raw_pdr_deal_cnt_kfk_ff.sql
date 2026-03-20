{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_pdr_deal_cnt_kfk_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_PDR_DEAL_CNT_KFK_FF',
        'target_table': 'ST_INT_RAW_PDR_DEAL_CNT_KFK_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.924145+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_pdr_deal_cntct_kfk AS (
    SELECT
        parent_id,
        contact_email_id,
        contact_first_name,
        contact_last_name,
        contact_phone_number,
        contact_job_title,
        contact_url,
        primary_flag,
        contact_cco_id,
        other_job_title,
        object_id,
        contact_type,
        contact_type_name,
        created_by,
        created_on,
        edw_updated_date,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_pdr_deal_cntct_kfk') }}
),

final AS (
    SELECT
        parent_id,
        contact_email_id,
        contact_first_name,
        contact_last_name,
        contact_phone_number,
        contact_job_title,
        contact_url,
        primary_flag,
        contact_cco_id,
        other_job_title,
        object_id,
        contact_type,
        contact_type_name,
        created_by,
        created_on,
        edw_updated_date,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_ff_st_int_raw_pdr_deal_cntct_kfk
)

SELECT * FROM final