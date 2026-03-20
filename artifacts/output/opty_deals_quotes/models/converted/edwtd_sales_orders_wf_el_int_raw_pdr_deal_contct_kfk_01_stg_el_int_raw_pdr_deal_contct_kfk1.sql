{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_pdr_deal_contct_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_PDR_DEAL_CONTCT_KFK',
        'target_table': 'EL_INT_RAW_PDR_DEAL_CONTCT_KFK1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.909287+00:00'
    }
) }}

WITH 

source_el_int_raw_pdr_deal_contct_kfk1 AS (
    SELECT
        object_id,
        quote_object_id,
        contact_email_id,
        contact_first_name,
        contact_last_name,
        contact_phone_number,
        contact_job_title,
        other_job_title,
        contact_type,
        contact_type_name,
        contact_cco_id,
        created_on,
        created_by,
        updated_on,
        updated_by,
        primary_flag,
        contact_url,
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
    FROM {{ source('raw', 'el_int_raw_pdr_deal_contct_kfk1') }}
),

final AS (
    SELECT
        object_id,
        quote_object_id,
        contact_email_id,
        contact_first_name,
        contact_last_name,
        contact_phone_number,
        contact_job_title,
        other_job_title,
        contact_type,
        contact_type_name,
        contact_cco_id,
        created_on,
        created_by,
        updated_on,
        updated_by,
        primary_flag,
        contact_url,
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
    FROM source_el_int_raw_pdr_deal_contct_kfk1
)

SELECT * FROM final