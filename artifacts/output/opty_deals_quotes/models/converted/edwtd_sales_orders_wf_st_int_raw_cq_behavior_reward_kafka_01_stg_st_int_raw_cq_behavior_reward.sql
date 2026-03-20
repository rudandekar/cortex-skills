{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_behavior_reward_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_BEHAVIOR_REWARD_KAFKA',
        'target_table': 'ST_INT_RAW_CQ_BEHAVIOR_REWARD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.941691+00:00'
    }
) }}

WITH 

source_el_int_rw_cq_behvior_rwrd_kfk AS (
    SELECT
        object_id,
        bh_reward_code,
        bh_reward_version,
        deal_object_id,
        program_name,
        bundle_type,
        bundle_type_desc,
        created_on,
        created_by,
        updated_on,
        updated_by,
        batch_id,
        create_datetime,
        action_code,
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
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user,
        auto_approved_flag
    FROM {{ source('raw', 'el_int_rw_cq_behvior_rwrd_kfk') }}
),

final AS (
    SELECT
        object_id,
        bh_reward_code,
        bh_reward_version,
        deal_object_id,
        program_name,
        bundle_type,
        bundle_type_desc,
        created_on,
        created_by,
        updated_on,
        updated_by,
        batch_id,
        create_datetime,
        action_code,
        quote_object_id,
        bh_reward_id,
        bh_reward_name,
        is_bh_reward_selectable,
        is_bh_reward_selected,
        bh_reward_version_applied,
        is_bh_reward_approved,
        auto_approved_flag
    FROM source_el_int_rw_cq_behvior_rwrd_kfk
)

SELECT * FROM final