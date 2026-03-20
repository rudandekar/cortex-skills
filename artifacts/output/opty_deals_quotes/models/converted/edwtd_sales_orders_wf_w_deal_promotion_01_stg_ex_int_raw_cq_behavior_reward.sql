{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_deal_promotion', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_PROMOTION',
        'target_table': 'EX_INT_RAW_CQ_BEHAVIOR_REWARD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.991248+00:00'
    }
) }}

WITH 

source_st_int_raw_cq_behavior_reward AS (
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
    FROM {{ source('raw', 'st_int_raw_cq_behavior_reward') }}
),

transformed_exptrans AS (
    SELECT
    object_id,
    bh_reward_code,
    bh_reward_version,
    deal_object_id,
    created_on,
    program_name,
    bundle_type,
    bundle_type_desc,
    created_by,
    updated_on,
    updated_by,
    batch_id,
    create_datetime,
    action_code,
    exception_type,
    quote_object_id,
    bh_reward_id,
    bh_reward_name,
    is_bh_reward_selectable,
    is_bh_reward_selected,
    bh_reward_version_applied,
    is_bh_reward_approved,
    auto_approved_flag
    FROM source_st_int_raw_cq_behavior_reward
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
        exception_type,
        quote_object_id,
        bh_reward_id,
        bh_reward_name,
        is_bh_reward_selectable,
        is_bh_reward_selected,
        bh_reward_version_applied,
        is_bh_reward_approved,
        auto_approved_flag
    FROM transformed_exptrans
)

SELECT * FROM final