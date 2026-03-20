{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_qf_response_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_QF_RESPONSE_KAFKA_FF',
        'target_table': 'ST_CQ_QF_RESPONSE_KAFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.877109+00:00'
    }
) }}

WITH 

source_st_cq_qf_response_kafka_ff AS (
    SELECT
        parent_id,
        question_id,
        value_name,
        last_update_date,
        last_updated_by,
        create_datetime,
        created_by,
        creation_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_cq_qf_response_kafka_ff') }}
),

final AS (
    SELECT
        parent_id,
        question_id,
        value_name,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        last_update_date,
        last_updated_by,
        create_datetime,
        created_by,
        creation_date
    FROM source_st_cq_qf_response_kafka_ff
)

SELECT * FROM final