{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_competitor_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_COMPETITOR_KAFKA_FF',
        'target_table': 'ST_CQ_COMPETITOR_KAFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.984545+00:00'
    }
) }}

WITH 

source_st_cq_competitor_kafka_ff AS (
    SELECT
        parent_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        initiated_source,
        object_id,
        source,
        dm_update_date,
        create_datetime,
        created_by,
        created_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_cq_competitor_kafka_ff') }}
),

final AS (
    SELECT
        parent_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        initiated_source,
        object_id,
        source,
        message_sequence_number,
        dm_update_date,
        create_datetime,
        created_by,
        created_on
    FROM source_st_cq_competitor_kafka_ff
)

SELECT * FROM final