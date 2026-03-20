{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_bhvr_reward_kafka_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_BHVR_REWARD_KAFKA_FF',
        'target_table': 'ST_INT_RAW_CQ_BHVR_RWRD_KFKA_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.936307+00:00'
    }
) }}

WITH 

source_st_int_raw_cq_behavior_reward AS (
    SELECT
        parent_id,
        objectid,
        quoteobject_id,
        deal_object_id,
        bh_reward_id,
        bh_reward_code,
        bh_reward_name,
        bh_reward_version,
        program_name,
        is_bh_reward_selectable,
        is_bh_reward_selected,
        bundle_type,
        bh_reward_version_applied,
        is_bh_reward_approved,
        bundle_type_desc,
        create_datetime,
        created_by,
        created_on,
        auto_approved_flag,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_int_raw_cq_behavior_reward') }}
),

final AS (
    SELECT
        parent_id,
        object_id,
        bh_reward_code,
        bh_reward_version,
        deal_object_id,
        program_name,
        bundle_type,
        quote_object_id,
        bh_reward_id,
        bh_reward_name,
        is_bh_reward_selectable,
        is_bh_reward_selected,
        bh_reward_version_applied,
        is_bh_reward_approved,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        bundle_type_desc,
        create_datetime,
        created_by,
        created_on,
        auto_approved_flag
    FROM source_st_int_raw_cq_behavior_reward
)

SELECT * FROM final