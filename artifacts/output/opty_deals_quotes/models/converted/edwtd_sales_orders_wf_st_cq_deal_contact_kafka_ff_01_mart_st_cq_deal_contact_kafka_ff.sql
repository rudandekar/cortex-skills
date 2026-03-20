{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_deal_contact_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_DEAL_CONTACT_KAFKA_FF',
        'target_table': 'ST_CQ_DEAL_CONTACT_KAFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.911767+00:00'
    }
) }}

WITH 

source_st_cq_deal_contact_kafka_ff AS (
    SELECT
        parent_id,
        object_id,
        contact_type,
        contact_name,
        contact_postn_id,
        contact_id,
        contact_row,
        dm_update_date,
        create_datetime,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_cq_deal_contact_kafka_ff') }}
),

final AS (
    SELECT
        parent_id,
        object_id,
        contact_type,
        contact_name,
        contact_postn_id,
        contact_id,
        contact_row,
        dm_update_date,
        create_datetime,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_st_cq_deal_contact_kafka_ff
)

SELECT * FROM final