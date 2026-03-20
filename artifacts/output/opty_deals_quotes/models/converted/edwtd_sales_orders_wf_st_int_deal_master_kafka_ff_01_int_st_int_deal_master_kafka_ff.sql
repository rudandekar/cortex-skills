{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_deal_master_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DEAL_MASTER_KAFKA_FF',
        'target_table': 'ST_INT_DEAL_MASTER_KAFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.918703+00:00'
    }
) }}

WITH 

source_st_int_deal_master_kafka AS (
    SELECT
        parent_id,
        area,
        market_segment,
        theater,
        share_node_id,
        level6_node,
        updated_by,
        updated_on,
        opty_number,
        theater_node_id,
        area_node_id,
        region_node_id,
        created_by,
        created_on,
        region,
        deal_type,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_int_deal_master_kafka') }}
),

final AS (
    SELECT
        parent_id,
        area,
        market_segment,
        theater,
        share_node_id,
        level6_node,
        updated_by,
        updated_on,
        opty_number,
        theater_node_id,
        area_node_id,
        region_node_id,
        created_by,
        created_on,
        region,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        deal_type
    FROM source_st_int_deal_master_kafka
)

SELECT * FROM final