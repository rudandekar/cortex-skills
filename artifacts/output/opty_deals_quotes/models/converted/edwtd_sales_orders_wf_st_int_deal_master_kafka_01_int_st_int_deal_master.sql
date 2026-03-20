{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_deal_master_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DEAL_MASTER_KAFKA',
        'target_table': 'ST_INT_DEAL_MASTER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.979920+00:00'
    }
) }}

WITH 

source_el_int_deal_master_kafka AS (
    SELECT
        opty_id,
        deal_type,
        share_node_id,
        created_by,
        created_on,
        updated_by,
        updated_on,
        theater,
        area,
        region,
        theater_node_id,
        area_node_id,
        region_node_id,
        opty_number,
        level6_node,
        market_segment,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_deal_master_kafka') }}
),

final AS (
    SELECT
        opty_id,
        deal_type,
        share_node_id,
        created_by,
        created_on,
        updated_by,
        updated_on,
        theater,
        area,
        region,
        theater_node_id,
        area_node_id,
        region_node_id,
        opty_number,
        level6_node,
        market_segment,
        edw_updated_date
    FROM source_el_int_deal_master_kafka
)

SELECT * FROM final