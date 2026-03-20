{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_deal_master_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_DEAL_MASTER_RNWL_KAFKA',
        'target_table': 'EL_INT_DEAL_MASTER_RNWL_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.984909+00:00'
    }
) }}

WITH 

source_el_int_deal_master_rnwl_kafka AS (
    SELECT
        opty_id,
        deal_type,
        share_node_id,
        theater,
        area,
        region,
        theater_node_id,
        area_node_id,
        region_node_id,
        opty_number,
        level6_node,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user,
        market_segment
    FROM {{ source('raw', 'el_int_deal_master_rnwl_kafka') }}
),

final AS (
    SELECT
        opty_id,
        deal_type,
        share_node_id,
        theater,
        area,
        region,
        theater_node_id,
        area_node_id,
        region_node_id,
        opty_number,
        level6_node,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user,
        market_segment
    FROM source_el_int_deal_master_rnwl_kafka
)

SELECT * FROM final